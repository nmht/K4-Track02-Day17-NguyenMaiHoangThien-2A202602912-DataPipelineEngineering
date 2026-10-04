# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Mai Hoàng Thiện - 2A202602912
**Repo:** `K4-Track02-Day17-HoVaTen-MSSV-DataPipelineEngineering`
**Commit bài nộp:** 
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Sử dụng AI để rà soát lỗi logic cập nhật bảng Silver, lỗi late data lookback, và lỗi trích xuất `ticket_id` từ sự kiện xóa của CDC. AI cũng hỗ trợ tổng hợp và hoàn thiện báo cáo.
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

|                               | Lỗi Silver                                                                                                                                                 | Lỗi late data                                                                                                                          | Lỗi xoá (CDC)                                                                                                                         |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**               | `test_silver_tickets_one_row_per_ticket` fail (assert 24 == 12) (T-91 cập nhật nhiều lần)                                                                  | `test_late_events_land_in_their_event_day` fail (thấy 2,1,0 != 5,3,1) (sự kiện u05 đến muộn)                                           | `test_cdc_delete_becomes_tombstone` fail (T-97 bị xoá)                                                                                |
| **Nguyên nhân gốc**           | Dùng `INSERT` ghi đè mọi thay đổi từ CDC vào `silver_tickets`, làm dữ liệu bị lặp thay vì cập nhật trạng thái mới nhất cho mỗi ticket.                     | Tham số `LOOKBACK_DAYS` đặt bằng 0, chỉ recompute ngày hiện tại nên lọt các event đến trễ so với lúc ingest (VD: u05 offline gửi sau). | Khi sự kiện mang `op='d'` (xoá), payload `after` bị rỗng. Việc chỉ lấy ID từ `after` trả về `null`, dẫn đến pipeline mất dấu bản ghi. |
| **Cách sửa** (file, vài dòng) | File `pipeline/silver.py`: Đổi `INSERT` thành `MERGE INTO ... USING _latest_changes ... ON ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`. | File `pipeline/config.py`: Đổi `LOOKBACK_DAYS = 0` thành `LOOKBACK_DAYS = 3` (dựa theo độ trễ P99 đo được).                            | File `pipeline/staging.py`: Sửa truy vấn ID thành `COALESCE(j->'value'->'after'->>'ticket_id', j->'value'->'before'->>'ticket_id')`.  |
| **Khái niệm trên slide**      | "Silver — Có khoá" (Thực thể có khóa duy nhất) và "Bốn cách viết idempotent" (dùng MERGE/UPSERT).                                                          | "Data về muộn" (Late-arriving data) và "Microbatch" (dùng lookback xử lý data đến trễ).                                                | "CDC log-based" (Cấu trúc payload) và "Xoá phải lan" (Delete propagation tạo tombstone).                                              |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (P50=0.00, P95=2.90) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: `silver_tickets` cần cập nhật trạng thái mới nhất (SCD2/upsert), còn `gold_feature_daily` là bảng tổng hợp theo ngày (event\_time) nên ghi đè cả ngày (microbatch) sẽ an toàn và sạch sẽ hơn.
- Tombstone thay vì xoá hẳn hàng trong Silver: Đánh dấu xóa mềm (is\_deleted = true) giúp luồng downstream (VD: tạo RAG index, ML) biết rõ bản ghi đã bị xóa để xử lý tiếp, tránh tình trạng mất dấu bản ghi một cách im lặng.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo dữ liệu là Point-in-time chính xác tại thời điểm sinh ra, giúp ML model không bị rò rỉ dữ liệu (data leakage) từ tương lai khi training.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Dữ liệu bài toán nhỏ gọn, DuckDB chạy in-memory đa luồng cho tốc độ cực cao, ít phụ thuộc hạ tầng phức tạp, chi phí vận hành rẻ hơn nhiều so với Spark.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   \=> Cần tách bạch Snapshot bất biến phục vụ ML/Audit và quy định xóa dữ liệu. Thay vì xóa PII trực tiếp, ta lưu riêng metadata/PII với ID tham chiếu, và khi người dùng yêu cầu xóa, ta chỉ hard-delete phần PII hoặc chạy một cron job xóa dữ liệu/mã hóa ở tất cả snapshot định kỳ theo chính sách (ví dụ 30 ngày), đảm bảo quyền lợi xóa trong khi vẫn giữ cấu trúc model hợp lệ.
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   \=> Cần chặn ở biên giới giữa Staging và Silver. Nên dùng một thư viện/API phân tích thực thể định danh (NER) như Presidio (Microsoft) để mask tên riêng. Quá trình kiểm tra độ phủ có thể được đo qua các data contract/quality gate, đếm tỉ lệ số lượng ticket nghi ngờ còn chứa thông tin PII bất thường.

## 5. Output (dán nguyên văn)

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 6.70s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ cd dbt_project ; ..\.venv\Scripts\dbt.exe build --event-time-start 2026-08-10 --event-time-end 2026-08-17
07:10:49  Running with dbt=1.12.5
07:10:51  Registered adapter: duckdb=1.11.0
07:10:59  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
07:10:59  
07:10:59  Concurrency: 1 threads (target='dev')
07:10:59  
07:11:04  1 of 19 START sql view model main.stg_events ................................... [RUN]
07:11:04  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.24s]
07:11:04  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
07:11:04  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.11s]
07:11:04  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
07:11:04  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.28s]
07:11:04  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
07:11:05  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.39s]
07:11:05  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
07:11:05  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.41s]
07:11:05  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
07:11:05  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.08s]
07:11:05  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
07:11:05  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.04s]
07:11:05  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
07:11:05  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.05s]
07:11:05  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
07:11:06  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.11s]
07:11:06  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
07:11:06  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.08s]
07:11:06  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
07:11:06  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.07s]
07:11:06  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
07:11:06  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.04s]
07:11:06  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
07:11:06  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.04s]
07:11:06  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
07:11:06  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.05s]
07:11:06  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
07:11:06  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.05s]
07:11:06  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
07:11:06  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
07:11:06  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.11s]
07:11:06  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
07:11:06  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.08s]
07:11:06  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
07:11:06  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.08s]
07:11:06  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
07:11:06  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.09s]
07:11:06  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
07:11:06  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.08s]
07:11:06  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
07:11:07  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.08s]
07:11:07  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
07:11:07  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.09s]
07:11:07  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.71s]
07:11:07  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
07:11:07  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.10s]
07:11:07  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
07:11:07  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.06s]
07:11:07  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
07:11:07  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.06s]
07:11:07  
07:11:07  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 7.82 seconds (7.82s).
07:11:07  
07:11:07  Completed successfully
07:11:07  
07:11:07  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
