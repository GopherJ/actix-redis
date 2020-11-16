use actix::{
    fut::{err, ok},
    prelude::*,
};
use backoff::{backoff::Backoff, ExponentialBackoff};
use log::{error, info, warn};
use serde::{de::DeserializeOwned, ser::Serialize};

use std::marker::PhantomData;

pub mod bb8_redis {
    pub use bb8_redis::*;
}

pub use self::bb8_redis::{
    bb8,
    redis::{
        cmd, ErrorKind, FromRedisValue, RedisError, RedisResult, ToRedisArgs, Value as RedisValue,
    },
    RedisConnectionManager, RedisPool,
};

pub enum RedisCmd<T: Serialize, R: DeserializeOwned> {
    Set(String, T),
    SetEx(String, T, usize),
    SetNx(String, T),
    Get(String),
    Del(String),
    Hget(String, String),
    Hset(String, String, T),
    HsetNx(String, String, T),
    Hincrby(String, String, i64),
    Hdel(String, String),
    Sadd(String, T),
    Scard(String),
    Smembers(String),
    Sismember(String, T),
    Srem(String, T),
    Zadd(String, i64, T),
    Zrem(String, T),
    Zcard(String),
    Zrangebyscore(String, i64, i64, usize, usize),
    Private(PhantomData<R>),
}

impl<T: Serialize + Unpin + 'static, R: DeserializeOwned + Unpin + 'static> Message
    for RedisCmd<T, R>
{
    type Result = RedisResult<R>;
}

pub struct RedisClient {
    addr: String,
    backoff: ExponentialBackoff,
    pool: Option<RedisPool>,
}

impl RedisClient {
    pub fn new<S: Into<String>>(addr: S) -> RedisClient {
        let addr = addr.into();

        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = None;

        RedisClient {
            addr,
            backoff,
            pool: None,
        }
    }
}

impl Actor for RedisClient {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        match RedisConnectionManager::new(self.addr.as_str()) {
            Ok(manager) => {
                bb8::Pool::builder()
                    .build(manager)
                    .into_actor(self)
                    .map(|res, act, ctx| match res {
                        Ok(pool) => {
                            info!("Connected to redis_client server: {}", act.addr);
                            act.pool = Some(RedisPool::new(pool));
                            act.backoff.reset();
                        }
                        Err(err) => {
                            error!("Cannot build bb8 redis_client pool: {}", err);
                            if let Some(timeout) = act.backoff.next_backoff() {
                                ctx.run_later(timeout, |_, ctx| ctx.stop());
                            }
                        }
                    })
                    .wait(ctx);
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
    }
}

macro_rules! to_redis_args {
    ($val:ident) => {
        serde_json::to_string(&$val).map_err(|_| {
            RedisError::from((ErrorKind::IoError, "Not able to serialize as redis args"))
        })?
    };
}

macro_rules! from_redis_value {
    ($val:ident) => {
        serde_json::from_str(&$val).map_err(|_| {
            RedisError::from((ErrorKind::IoError, "Not able to deserialize as redis value"))
        })
    };
}

impl<T: Serialize + Unpin + 'static, R: DeserializeOwned + Unpin + 'static> Handler<RedisCmd<T, R>>
    for RedisClient
{
    type Result = ResponseActFuture<Self, RedisResult<R>>;

    fn handle(&mut self, msg: RedisCmd<T, R>, _: &mut Self::Context) -> Self::Result {
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
                    RedisCmd::Set(key, val) => {
                        cmd("SET")
                            .arg(key)
                            .arg(to_redis_args!(val))
                            .query_async(conn)
                            .await
                    }
                    RedisCmd::SetEx(key, val, ttl) => {
                        cmd("SET")
                            .arg(key)
                            .arg(to_redis_args!(val))
                            .arg("EX")
                            .arg(ttl)
                            .query_async(conn)
                            .await
                    }
                    RedisCmd::SetNx(key, val) => {
                        cmd("SETNX")
                            .arg(key)
                            .arg(to_redis_args!(val))
                            .query_async(conn)
                            .await
                    }
                    RedisCmd::Hget(key, field) => {
                        cmd("HGET").arg(key).arg(field).query_async(conn).await
                    }
                    RedisCmd::Hset(key, field, val) => {
                        cmd("HSET")
                            .arg(key)
                            .arg(field)
                            .arg(to_redis_args!(val))
                            .query_async(conn)
                            .await
                    }
                    RedisCmd::HsetNx(key, field, val) => {
                        cmd("HSETNX")
                            .arg(key)
                            .arg(field)
                            .arg(to_redis_args!(val))
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
                        cmd("SADD")
                            .arg(key)
                            .arg(to_redis_args!(val))
                            .query_async(conn)
                            .await
                    }
                    RedisCmd::Smembers(key) => cmd("SMEMBERS").arg(key).query_async(conn).await,
                    RedisCmd::Sismember(key, val) => {
                        cmd("SMEMBERS")
                            .arg(key)
                            .arg(to_redis_args!(val))
                            .query_async(conn)
                            .await
                    }
                    RedisCmd::Scard(key) => cmd("SCARD").arg(key).query_async(conn).await,
                    RedisCmd::Srem(key, val) => {
                        cmd("SREM")
                            .arg(key)
                            .arg(to_redis_args!(val))
                            .query_async(conn)
                            .await
                    }
                    RedisCmd::Zadd(key, score, val) => {
                        cmd("ZADD")
                            .arg(key)
                            .arg(score)
                            .arg(to_redis_args!(val))
                            .query_async(conn)
                            .await
                    }
                    RedisCmd::Zrem(key, val) => {
                        cmd("ZREM")
                            .arg(key)
                            .arg(to_redis_args!(val))
                            .query_async(conn)
                            .await
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
                    RedisCmd::Private(_) => panic!("Private command. It should only be used internally for specifing return value's type"),
                }
            }
            .into_actor(self)
            .then(|res, act, ctx| match res
            .and_then(|v: RedisValue| String::from_redis_value(&v))
            .and_then(|payload: String| from_redis_value!(payload)) {
                Ok(res_ok) => ok(res_ok),
                Err(res_err) => {
                    if res_err.is_io_error() {
                        warn!("Redis connection error: {} error: {}", act.addr, res_err);
                        ctx.stop();
                    }
                    err(res_err)
                }
            });
            Box::new(res)
        } else {
            Box::new(err(RedisError::from((ErrorKind::IoError, "Not connected"))))
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
