use std::net::SocketAddr;

use actix::{
    fut::{err, ok},
    prelude::*,
};
use backoff::{backoff::Backoff, ExponentialBackoff};
use bb8_redis::{
    bb8,
    redis::{cmd, ErrorKind, RedisError, RedisResult, ToRedisArgs, Value as RedisValue},
    RedisConnectionManager, RedisPool,
};
use log::{error, info, warn};
use redis_async::{
    client::pubsub::PubsubStream,
    client::{pubsub_connect, PubsubConnection},
    error::Error,
    resp::RespValue,
};

pub type StreamHandlerFn = fn(item: Result<RespValue, Error>);

pub enum RedisCmd<T: ToRedisArgs> {
    Set(String, T),
    SetEx(String, String, T),
    SetNx(String, T),
    Get(String),
    Del(String),
    Hget(String, String),
    Hset(String, String, T),
    HsetNx(String, String, T),
    Hincrby(String, String, T),
    Hdel(String, String),
    Sadd(String, T),
    Scard(String),
    Smembers(String),
    Sismember(String, T),
    Srem(String, T),
    Zadd(String, T, T),
    Zrem(String, T),
    Zcard(String),
    Zrangebyscore(String, T, T, T, T),
}

impl<T: ToRedisArgs + Unpin + 'static> Message for RedisCmd<T> {
    type Result = RedisResult<RedisValue>;
}

pub struct RedisClient {
    url: String,
    addr: SocketAddr,
    backoff: ExponentialBackoff,
    pool: Option<RedisPool>,
    pubsub: Option<PubsubConnection>,
    sub_topics: Option<Vec<String>>,
    psub_topics: Option<Vec<String>>,
    stream_handler_fn: Option<StreamHandlerFn>,
}

impl RedisClient {
    pub fn start<S: Into<String>>(
        url: S,
        sub_topics: Option<Vec<String>>,
        psub_topics: Option<Vec<String>>,
        stream_handler_fn: Option<StreamHandlerFn>,
    ) -> Addr<RedisClient> {
        let url = url.into();

        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = None;

        Supervisor::start(move |_| RedisClient {
            addr: url.parse().unwrap(),
            url,
            backoff,
            pool: None,
            pubsub: None,
            sub_topics,
            psub_topics,
            stream_handler_fn,
        })
    }
}

impl Actor for RedisClient {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        match RedisConnectionManager::new(self.url.as_str()) {
            Ok(manager) => {
                bb8::Pool::builder()
                    .build(manager)
                    .into_actor(self)
                    .map(|res, act, _ctx| match res {
                        Ok(pool) => {
                            info!("Created connection pool to redis server: {}", act.url);
                            act.pool = Some(RedisPool::new(pool));
                        }
                        Err(err) => {
                            error!("Cannot build bb8 redis_client pool: {}", err);
                        }
                    })
                    .wait(ctx);

                let addr = self.addr.clone();
                let sub_topics = self.sub_topics.clone().unwrap_or(vec![]);
                let psub_topics = self.psub_topics.clone().unwrap_or(vec![]);

                async move {
                    let pubsub_conn = pubsub_connect(&addr).await?;
                    let mut sub_streams = vec![];
                    let mut psub_streams = vec![];

                    for sub_topic in sub_topics {
                        sub_streams.push(pubsub_conn.subscribe(&sub_topic).await?);
                    }

                    for psub_topic in psub_topics {
                        psub_streams.push(pubsub_conn.psubscribe(&psub_topic).await?);
                    }

                    Ok((pubsub_conn, sub_streams, psub_streams))
                }
                .into_actor(self)
                .map(
                    |res: Result<
                        (PubsubConnection, Vec<PubsubStream>, Vec<PubsubStream>),
                        Error,
                    >,
                     act,
                     ctx| match res {
                        Ok((pubsub_conn, sub_streams, psub_streams)) => {
                            info!("Created pubsub connection to redis server: {}", act.url);

                            for sub_stream in sub_streams {
                                ctx.add_stream(sub_stream);
                            }

                            for psub_stream in psub_streams {
                                ctx.add_stream(psub_stream);
                            }

                            act.pubsub = Some(pubsub_conn);
                        }
                        Err(err) => {
                            error!("Cannot build bb8 redis_client pool: {}", err);
                        }
                    },
                )
                .wait(ctx);

                if self.pool.is_some() && self.pubsub.is_some() {
                    self.backoff.reset();
                } else {
                    if let Some(timeout) = self.backoff.next_backoff() {
                        ctx.run_later(timeout, |_, ctx| ctx.stop());
                    }
                }
            }
            Err(err) => {
                error!("Cannot build bb8 redis_client manager: {}", err);
                if let Some(timeout) = self.backoff.next_backoff() {
                    ctx.run_later(timeout, |_, ctx| ctx.stop());
                }
            }
        }
    }
}

impl Supervised for RedisClient {
    fn restarting(&mut self, _: &mut Self::Context) {
        self.pool.take();
        self.pubsub.take();
    }
}

impl StreamHandler<Result<RespValue, Error>> for RedisClient {
    fn handle(&mut self, item: Result<RespValue, Error>, _ctx: &mut Context<RedisClient>) {
        if let Some(stream_handler_fn) = self.stream_handler_fn {
            stream_handler_fn(item)
        }
    }

    fn finished(&mut self, ctx: &mut Self::Context) {
        ctx.stop()
    }
}

impl<T: ToRedisArgs + Unpin + 'static> Handler<RedisCmd<T>> for RedisClient {
    type Result = ResponseActFuture<Self, RedisResult<RedisValue>>;

    fn handle(&mut self, msg: RedisCmd<T>, _: &mut Self::Context) -> Self::Result {
        if let Some(ref pool) = self.pool {
            let pool = pool.clone();

            let res = async move {
                let mut conn = pool.get().await.map_err(|_| {
                    RedisError::from((
                        ErrorKind::IoError,
                        "Not able to acquire connection from pool",
                    ))
                })?;
                let conn = conn.as_mut().ok_or(RedisError::from((
                    ErrorKind::IoError,
                    "Not able to acquire connection from pool",
                )))?;

                match msg {
                    RedisCmd::Get(key) => cmd("GET").arg(key).query_async(conn).await,
                    RedisCmd::Del(key) => cmd("DEL").arg(key).query_async(conn).await,
                    RedisCmd::Set(key, val) => cmd("SET").arg(key).arg(val).query_async(conn).await,
                    RedisCmd::SetEx(key, val, ttl) => {
                        cmd("SET")
                            .arg(key)
                            .arg(val)
                            .arg("EX")
                            .arg(ttl)
                            .query_async(conn)
                            .await
                    }
                    RedisCmd::SetNx(key, val) => {
                        cmd("SETNX").arg(key).arg(val).query_async(conn).await
                    }
                    RedisCmd::Hget(key, field) => {
                        cmd("HGET").arg(key).arg(field).query_async(conn).await
                    }
                    RedisCmd::Hset(key, field, val) => {
                        cmd("HSET")
                            .arg(key)
                            .arg(field)
                            .arg(val)
                            .query_async(conn)
                            .await
                    }
                    RedisCmd::HsetNx(key, field, val) => {
                        cmd("HSETNX")
                            .arg(key)
                            .arg(field)
                            .arg(val)
                            .query_async(conn)
                            .await
                    }
                    RedisCmd::Hincrby(key, field, val) => {
                        cmd("HINCRBY")
                            .arg(key)
                            .arg(field)
                            .arg(val)
                            .query_async(conn)
                            .await
                    }
                    RedisCmd::Hdel(key, field) => {
                        cmd("HDEL").arg(key).arg(field).query_async(conn).await
                    }
                    RedisCmd::Sadd(key, val) => {
                        cmd("SADD").arg(key).arg(val).query_async(conn).await
                    }
                    RedisCmd::Smembers(key) => cmd("SMEMBERS").arg(key).query_async(conn).await,
                    RedisCmd::Sismember(key, val) => {
                        cmd("SMEMBERS").arg(key).arg(val).query_async(conn).await
                    }
                    RedisCmd::Scard(key) => cmd("SCARD").arg(key).query_async(conn).await,
                    RedisCmd::Srem(key, val) => {
                        cmd("SREM").arg(key).arg(val).query_async(conn).await
                    }
                    RedisCmd::Zadd(key, score, val) => {
                        cmd("ZADD")
                            .arg(key)
                            .arg(score)
                            .arg(val)
                            .query_async(conn)
                            .await
                    }
                    RedisCmd::Zrem(key, val) => {
                        cmd("ZREM").arg(key).arg(val).query_async(conn).await
                    }
                    RedisCmd::Zcard(key) => cmd("ZCARD").arg(key).query_async(conn).await,
                    RedisCmd::Zrangebyscore(key, score_start, score_end, offset, count) => {
                        cmd("ZRANGEBYSCORE")
                            .arg(key)
                            .arg(score_start)
                            .arg(score_end)
                            .arg("LIMIT")
                            .arg(offset)
                            .arg(count)
                            .query_async(conn)
                            .await
                    }
                }
            }
            .into_actor(self)
            .then(|res, act, ctx| match res {
                Ok(res_ok) => ok(res_ok),
                Err(res_err) => {
                    if res_err.is_io_error() {
                        warn!("Redis connection error: {} error: {}", act.url, res_err);
                        ctx.stop();
                    }
                    err(res_err)
                }
            });
            Box::pin(res)
        } else {
            Box::pin(err(RedisError::from((ErrorKind::IoError, "Not connected"))))
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
