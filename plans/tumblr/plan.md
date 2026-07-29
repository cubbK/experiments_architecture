Here's the full plan:

---

## Plan: Tumblr Clone — Feed, Tag Feed & Search at Scale

**TL;DR:** Greenfield FastAPI app that implements the exact architectural patterns Tumblr actually uses: a Kafka firehose → hybrid fanout workers → Redis inbox model for feeds, Redis ZSETs for tag feeds, and Elasticsearch for full-text search. The goal is to make every pattern learnable and production-traceable, not just a toy.

---

### Phase 1 — Foundation (_start here; everything depends on this_)

1. **`docker-compose.yml`** — stand up the core infrastructure:
   - Redis for feed/tag cache storage and post hydration
   - Kafka 3 in KRaft mode, no Zookeeper, with a `posts` topic ready for producers/consumers
   - Elasticsearch 8 for full-text search indexing
   - Shared network, persistent volumes, and health checks for all three services
   - Environment variables and bootstrap defaults documented so the backend can connect cleanly in later phases

---

### Phase 2 — Feed System (_core Tumblr pattern_)

7. **Kafka producer** in post service: every `POST /posts` emits a `post.created` event to the `posts` Kafka topic (payload: post_id, author_id, tags, timestamp)
8. **FanoutWorker** (`app/workers/fanout_worker.py`) — aiokafka consumer:
   - Check `author.follower_count` against `CELEBRITY_THRESHOLD` (default 10,000)
   - **Regular users (push path):** `ZADD feed:{follower_id} {unix_ms} {post_id}` for every follower; `ZREMRANGEBYRANK` trims inbox to last 1,000 entries
   - **Celebrities:** skip push entirely — their fans get posts merged at read time
9. **Post cache** — on creation, `SET post:{id}` JSON in Redis with TTL (avoids Postgres round-trip on every feed hydration)
10. **`GET /feed`** (auth required, cursor-based) in `app/services/feed_service.py`:
    - `ZREVRANGEBYSCORE feed:{user_id}` from cursor score (unix_ms) — O(log N)
    - For each followed celebrity: `ZREVRANGEBYSCORE user_posts:{celebrity_id}` (a separate ZSET written on post creation)
    - Merge + sort in Python; hydrate post objects from Redis cache, fallback to Postgres

---

### Phase 3 — Tag Feed (_parallel with Phase 2 worker work_)

11. **TagWorker** (same fanout worker process, separate consumer): on `post.created` event, for each tag → `ZADD tag:{slug} {unix_ms} {post_id}`
12. **`GET /tags/{slug}/posts`** — `ZREVRANGEBYSCORE tag:{slug}` with score cursor; returns paginated posts
13. **Trending tags** — on every post creation, `ZINCRBY trending:tags 1 {slug}`; `GET /tags/trending` returns `ZREVRANGE trending:tags 0 19`

---

### Phase 4 — Search

14. **SearchIndexerWorker** (`app/workers/search_indexer.py`) — aiokafka consumer, indexes to Elasticsearch:
    - Index `posts`, mapping: `body` (text, analyzed), `tags` (keyword array), `user_id` (keyword), `created_at` (date)
15. **`GET /search?q=&tag=&user=&from=&size=`** — ES `query_string` on `body` in query context + `term` filters in filter context + `date` sort; returns paginated results
16. **Delete propagation** — on `DELETE /posts/{id}`: remove from ES + `ZREM` from all affected Redis ZSETs + set Redis tombstone

---

### Key Scale Patterns You'll Have Hands-On Experience With

| Pattern                                                     | Where                                             |
| ----------------------------------------------------------- | ------------------------------------------------- |
| **Inbox model** (not scatter-gather)                        | `feed_service.py` — O(log N) read from Redis ZSET |
| **Hybrid fanout** (push vs. pull by celebrity threshold)    | `fanout_service.py`                               |
| **Kafka firehose** (decoupled writes)                       | `kafka_client.py` + workers                       |
| **Cursor pagination** (score-based, not SQL OFFSET)         | All feed/tag endpoints                            |
| **Feed trimming** (bounded inbox size)                      | `ZREMRANGEBYRANK` in fanout worker                |
| **Selective materialization** (Yahoo/Tumblr paper)          | celebrity merge in `feed_service.py`              |
| **Denormalized follower counts** (avoid N+1 at fanout time) | `users.follower_count` column                     |

---

Does this match what you had in mind? Any phases you want to expand, collapse, or reorder before we hand off to implementation?
