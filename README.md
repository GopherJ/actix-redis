# redis-actor

## Installation

Add this package to `Cargo.toml` of your project. (Check https://crates.io/crates/redis-actor for right version)

```toml
[dependencies]
actix = "0.9.0"
redis-actor = "0.2.0"
```

## Features

- built-in connection pool
- reconnecting

## Get started

```rust
use actix::{Arbiter, Addr, Supervisor};
use redis_actor::{RedisActor, RedisCmd, bb8_redis::redis::RedisResult};

let arb = Arbiter::new();
let redis_url = "http://127.0.0.1:6379/0"

let addr: Addr<RedisActor> = Supervisor::start_in_arbiter(
        &arb,
        move |_| RedisActor::new(redis_url).unwrap()
);

addr.send(RedisCmd::<String>::Set("hello".to_owned(), " world!".to_owned()).await;
```
