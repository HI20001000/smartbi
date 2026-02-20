# SmartBI 工作流驗證樣例（對齊 `sql/exmaple_data.sql` 實際數據）

> 目的：提供「一定能在示例資料中命中結果」的測試樣例，避免使用不存在資料區間（例如 2024-01）的查詢；並補齊每個校驗點的「不通過」樣例與業務導向優化案例。

## 0) 測試資料準備

1. 建立/重建示例庫：

```bash
mysql -u <user> -p < sql/exmaple_data.sql
```

2. 設定環境變數（最小）：

```bash
export LLM_BASE_URL=<your_llm_url>
export LLM_MODEL=<your_model>
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_USER=<user>
export DB_PASSWORD=<password>
export DB_NAME=smartbi_demo
```

3. 啟動 CLI：

```bash
python -m app.main
```

---

## 1) SmartBI 全流程分析（Step A ~ Step I）

### Step A：意圖識別（Intent Router）
- 目標：判斷是否是資料查詢請求、問候、或其他意圖。
- 風險：非查詢語句若誤判為查詢，會導致後續無效語義/SQL。

### Step B：特徵抽取（時間、指標、維度、過濾）
- 目標：從自然語言抽取 `metrics/dimensions/filters/time_start/time_end`。
- 風險：
  - 時間未抽到（而治理要求必填時間）。
  - filter 無法轉換為標準欄位（canonical）。

### Step C：Token 匹配（語義層詞彙命中）
- 目標：把詞彙對齊語義層中的 dataset/metric/dimension/entity。
- 風險：命中不到指標或維度，或命中敏感欄位（blocked）。

### Step D1/D2：LLM 選擇 + Deterministic 合併
- 目標：在候選集內做選擇，並由 deterministic 規則做白名單化、補缺、正規化。
- 風險：
  - LLM 選了候選外欄位。
  - filter 欄位不合法或與資料集不一致。

### Step E：語義校驗（Governance + 可編譯性）
- 目標：執行規則校驗（時間必填、PII 禁止、多資料集 join、filter shape）。
- 風險：
  - `BLOCKED_MATCH`、`TIME_FILTER_REQUIRED`、`INVALID_CANONICAL_REF`。
  - `MULTI_DATASET_NO_JOIN_PATH`、`NO_COMPILABLE_SELECT`。

### Step F：SQL 生成
- 目標：基於 canonical plan 生成穩定 SQL。
- 風險：join/on 不完整、條件錯欄位、時間條件被覆寫。

### Step G：SQL 執行與結果集
- 目標：執行 SQL 並回傳結果。
- 風險：空結果（特別是時間超界），需進行診斷與自動修正。

### Step H：圖表規劃
- 目標：根據欄位型態與查詢語義規劃 chart type。
- 風險：numeric-only 結果誤退回 table，或時間序列未規劃為 line。

### Step I：圖表渲染
- 目標：輸出圖檔（預設 `artifacts/charts/query_chart.png`）。
- 風險：欄位映射不正確導致渲染失敗或觀測不清晰。

---

## 2) 示例資料可用時間與關鍵事實（先核對）

`exmaple_data.sql` 的核心分佈：

- 存款日快照：**2026-01-01、2026-01-02、2026-01-31**。
- 交易資料：集中在 **2026-01-01 / 2026-01-02 / 2026-01-31**。
- 放款日快照：**2026-01-01、2026-01-31**。
- 信用分月度：**2025-12、2026-01**。

可人工核對基準值：

- 2026-01-01 存款總餘額 = **365000.00**
- 2026-01-02 存款總餘額 = **403000.00**
- 2026-01-31 存款總餘額 = **450000.00**
- 2026-01 全期交易淨額 = **37500.00**
- 2026-01-31 逾期金額總和 = **19500.00**
- 2026-01 平均信用分 = **688.0**

---

## 3) 端到端「通過」樣例（E2E）

### Case E2E-01：時間序列折線圖（存款日趨勢）
- **輸入**：`查詢 2026年1月 每日存款餘額趨勢`
- **預期**：
  - `chart_type='line'`
  - 覆蓋 2026-01-01 / 2026-01-02 / 2026-01-31
  - 序列核對：365000 → 403000 → 450000

### Case E2E-02：類別維度條形圖（分行）
- **輸入**：`統計 2026年1月31日 各分行存款餘額`
- **預期**：
  - `chart_type='bar'`
  - 命中 `branch.branch_name` 或 `branch.region`
  - 1/31 分行值：澳門半島=300000、氹仔=70000、路氹城=52000、路環=28000

### Case E2E-03：數值-only 仍輸出條形圖
- **輸入**：`查詢 2026年1月 澳門半島存款餘額總額`
- **預期**：
  - 單 metric 不退回 `table`
  - `chart_type='bar'`
  - x 軸使用 `__row_index__`

### Case E2E-04：交易主題驗證（交易淨額）
- **輸入**：`查詢 2026年1月 交易淨額`
- **預期**：
  - 命中 `transactions.net_txn_amount`
  - 可核對 = 37500.00

### Case E2E-05：放款風險主題（逾期金額）
- **輸入**：`查詢 2026年1月31日 逾期金額`
- **預期**：
  - 命中 `loans_daily_balance.overdue_amount`
  - 可核對 = 19500.00

### Case E2E-06：信用分月度主題
- **輸入**：`查詢 2026年1月 平均信用分`
- **預期**：
  - 命中 `credit_score_monthly.avg_credit_score`
  - 可核對 = 688.0

### Case E2E-07：時間超界自動修正
- **輸入**：`查詢 2024年全年 澳門半島存款餘額`
- **預期**：
  - 初次可能空結果
  - 提示「查詢時間範圍可能超出資料可用區間」
  - 自動改用 2026-01 後重試

### Case E2E-08：PII 治理
- **輸入**：`查詢客戶姓名與身份證號`
- **預期**：
  - 被治理攔截/拒答
  - 不得生成含 `full_name` / `id_no` SQL

---

## 4) 每個校驗點的「不通過」樣例設計

> 下表聚焦 Step E 校驗碼，並用可操作輸入覆蓋常見失敗。

| 校驗碼 | 不通過樣例（建議輸入） | 預期失敗原因 |
|---|---|---|
| `BLOCKED_MATCH` | `查詢 2026年1月 客戶身份證號清單` | 命中敏感欄位 `customer.id_no`，必須拒答 |
| `TIME_FILTER_REQUIRED` | `查詢 存款餘額總額` | 未提供時間範圍，治理要求時間條件 |
| `TIME_AXIS_INCOMPLETE` | （診斷注入）僅有 start_date 無 end_date | 時間軸不完整 |
| `EMPTY_SELECTION` | `幫我看一下` | 未選出任何 metric/dimension |
| `INVALID_CANONICAL_REF` | `查詢 2026年1月 ghost_metric` | 指向語義層不存在欄位 |
| `MULTI_DATASET_NO_JOIN_PATH` | 同時查兩個無共同 join entity 的資料集指標 | 多資料集無可連接路徑 |
| `DATASET_MISMATCH` | 主資料集 `sales` + 維度 `other.region` | 所選欄位與 primary dataset 不一致 |
| `INVALID_FILTER_SHAPE` | `查詢 2026年1月存款，條件: ???` | filter 不是合法 object/expr |
| `INVALID_FILTER_BETWEEN` | `日期 between 2026-01-01` | between 值數量不為 2 |
| `INVALID_FILTER_VALUE` | `地區 in()` | in 無任何值 |
| `NO_COMPILABLE_SELECT` | 只選 entity 維度、無可編譯 dataset select item | 無法生成有效 SELECT |

---

## 5) 由業務角度補充/優化樣例（新增）

### BIZ-01：幣別觀點（MOP/HKD）
- **輸入**：`查詢 2026年1月31日 各幣別存款餘額`
- **價值**：澳門場景常見雙幣別，驗證 account.currency 維度與金額解讀。

### BIZ-02：凍結金額監控（資金可用性）
- **輸入**：`查詢 2026年1月31日 凍結金額占存款餘額比`
- **價值**：反映賬戶風險與可動用資金狀態。

### BIZ-03：渠道交易結構（營運）
- **輸入**：`查詢 2026年1月 按渠道交易筆數與淨額`
- **價值**：比較 BRANCH/ATM/MOBILE/WEB/API 的導流效果與收益質量。

### BIZ-04：逾期分層（風險）
- **輸入**：`查詢 2026年1月31日 各分行 DPD30 逾期金額`
- **價值**：直接對應催收優先級與資產質量管理。

### BIZ-05：信用分遷移（月度）
- **輸入**：`比較 2025-12 與 2026-01 信用分 band 分布`
- **價值**：觀察客群風險遷移與策略成效。

### BIZ-06：高風險客群與產品交叉
- **輸入**：`查詢 2026年1月 HIGH 風險等級客戶在貸餘額`
- **價值**：支持風險偏好與授信策略調整。

---

## 6) 單元測試覆蓋與執行

執行命令：

```bash
python -m unittest tests/test_chart_planner.py tests/test_semantic_pipeline.py tests/test_main_diagnostics.py
```

- `tests/test_chart_planner.py`：圖表規劃健壯性（numeric-only、空值首行等）
- `tests/test_semantic_pipeline.py`：新增校驗失敗案例，覆蓋治理/欄位/過濾形狀/多資料集/可編譯性
- `tests/test_main_diagnostics.py`：時間邊界診斷與自動修正

---

## 7) 驗證完成標準

- E2E 通過案例（E2E-01 ~ E2E-08）至少人工驗證 1 次。
- 校驗失敗案例（第 4 節）可穩定命中對應 error code。
- 單元測試全綠。
- 圖表輸出存在：`artifacts/charts/query_chart.png`（或 `CHART_OUTPUT_DIR` 指定路徑）。
