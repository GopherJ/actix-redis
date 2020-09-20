use actix::{
    fut::{err, ok},
    prelude::*,
};
use backoff::{backoff::Backoff, ExponentialBackoff};
use log::{error, info, warn};

use bb8_redis::{
    bb8,
    redis::{cmd, ErrorKind, RedisError, RedisResult, Value as RedisValue},
    RedisConnectionManager, RedisPool,
};

pub enum RedisCmd {
    Set(String, String),
    SetWithEx(String, String, i64),
    Get(String),
    Del(String),
}

impl Message for RedisCmd {
    type Result = RedisResult<RedisValue>;
}

pub struct RedisClient {
    addr: String,
    backoff: ExponentialBackoff,
    pool: Option<RedisPool>,
}

impl RedisClient {
    pub fn start<S: Into<String>>(addr: S) -> Addr<RedisClient> {
        let addr = addr.into();

        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = None;

        Supervisor::start(|_| RedisClient {
            addr,
            backoff,
            pool: None,
        })
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

impl Handler<RedisCmd> for RedisClient {
    type Result = ResponseActFuture<Self, RedisResult<RedisValue>>;

    fn handle(&mut self, msg: RedisCmd, _: &mut Self::Context) -> Self::Result {
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
                    RedisCmd::SetWithEx(key, val, ttl) => {
                        cmd("SET")
                            .arg(key)
                            .arg(val)
                            .arg("EX")
                            .arg(ttl)
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
                        warn!("Redis connection error: {} error: {}", act.addr, res_err);
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
