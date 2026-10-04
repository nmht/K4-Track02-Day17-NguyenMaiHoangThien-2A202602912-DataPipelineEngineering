# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Mai Hoàng Thiện - 2A202602912
**Repo:** 
**Commit bài nộp:**
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):**
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `test_silver_tickets_one_row_per_ticket` fail (assert 24 == 12) (T-91 cập nhật nhiều lần) | `test_late_events_land_in_their_event_day` fail (thấy 2,1,0 != 5,3,1) (sự kiện u05 đến muộn) | `test_cdc_delete_becomes_tombstone` fail (T-97 bị xoá) |
| **Nguyên nhân gốc** | | | |
| **Cách sửa** (file, vài dòng) | | | |
| **Khái niệm trên slide** | | | |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (P50=0.00, P95=2.90) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS / FAIL — Gold checksum: `________________`
- `make parity`: PARITY / MISMATCH

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`:
- Tombstone thay vì xoá hẳn hàng trong Silver:
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ:
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark:

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

## 5. Output (dán nguyên văn)

```text
$ make verify

$ make test

$ make rerun3

$ make lateness

$ make dbt

$ make parity
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
