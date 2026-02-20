# SmartBI 工作流驗證樣例（對齊 `sql/exmaple_data.sql` 實際數據）

> 目的：提供「一定能在示例資料中命中結果」的測試樣例，避免使用不存在資料區間（例如 2024-01）的查詢。

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

## 1) 示例資料的可用時間與關鍵事實（請先確認）

`exmaple_data.sql` 的核心數據分佈：

- 存款日快照：**2026-01-01、2026-01-02、2026-01-31**。
- 交易資料：集中在 **2026-01-01 / 2026-01-02 / 2026-01-31**。
- 放款日快照：**2026-01-01、2026-01-31**。
- 信用分月度：**2025-12、2026-01**。

可用於人工核對的基準值（建議）：

- 2026-01-01 存款總餘額 = **365000.00**
- 2026-01-02 存款總餘額 = **403000.00**
- 2026-01-31 存款總餘額 = **450000.00**
- 2026-01 全期交易淨額 = **37500.00**
- 2026-01-31 逾期金額總和 = **19500.00**
- 2026-01 平均信用分 = **688.0**

---

## 2) 端到端輸入樣例（Step A ~ Step I）

> 每個樣例都建議檢查：
> - `Step C Token 命中結果`
> - `Step D2 合併後計畫（Deterministic）`
> - `Step E 規則校驗`
> - `Step F SQL 生成結果`
> - `Step G SQL 執行筆數`
> - `Step H 圖表規劃`
> - `Step I 圖表輸出`

### Case E2E-01：時間序列折線圖（存款日趨勢）
- **輸入**：`查詢 2026年1月 每日存款餘額趨勢`
- **預期**：
  - `chart_type='line'`
  - 結果應覆蓋 2026-01-01 / 2026-01-02 / 2026-01-31
  - 可核對總餘額序列：365000 → 403000 → 450000

### Case E2E-02：類別維度條形圖（分行）
- **輸入**：`統計 2026年1月31日 各分行存款餘額`
- **預期**：
  - `chart_type='bar'`
  - 維度命中 `branch.branch_name` 或 `branch.region`
  - 分行值可核對（1/31）：澳門半島=300000、氹仔=70000、路氹城=52000、路環=28000

### Case E2E-03：數值-only 仍輸出條形圖
- **輸入**：`查詢 2026年1月 澳門半島存款餘額總額`
- **預期**：
  - 單一 metric 輸出時不退回 `table`
  - `Step H` 為 `chart_type='bar'`
  - x 軸使用 row index（內部 key: `__row_index__`）

### Case E2E-04：交易主題驗證（交易淨額）
- **輸入**：`查詢 2026年1月 交易淨額`
- **預期**：
  - 命中 `transactions.net_txn_amount`
  - 能生成 SQL 並執行
  - 聚合結果可核對為 37500.00

### Case E2E-05：放款風險主題驗證（逾期金額）
- **輸入**：`查詢 2026年1月31日 逾期金額`
- **預期**：
  - 命中 `loans_daily_balance.overdue_amount`
  - 結果可核對為 19500.00

### Case E2E-06：信用分月度主題
- **輸入**：`查詢 2026年1月 平均信用分`
- **預期**：
  - 命中 `credit_score_monthly.avg_credit_score`
  - 結果可核對為 688.0

### Case E2E-07：時間超界自動修正（診斷流程）
- **輸入**：`查詢 2024年全年 澳門半島存款餘額`
- **預期**：
  - 初次結果可能為空
  - 觸發診斷：`查詢時間範圍可能超出資料可用區間`
  - 自動修正到可用範圍（2026-01 區間）後重試

### Case E2E-08：安全與語義治理（PII 禁止）
- **輸入**：`查詢客戶姓名與身份證號`
- **預期**：
  - 應被語義治理攔截 / 拒答
  - 不得輸出包含 `full_name` / `id_no` 的 SQL

---

## 3) 覆蓋目前所有單元測試樣例的對照清單

執行命令：

```bash
python -m unittest tests/test_chart_planner.py tests/test_semantic_pipeline.py tests/test_main_diagnostics.py
```

### 測試樣例對照（16 項）

#### A. `tests/test_chart_planner.py`（3 項）
1. Decimal 指標應規劃為 bar
2. 第一列為空值、後續為數值時仍可識別數值欄
3. numeric-only 結果使用 row index 生成 bar

#### B. `tests/test_semantic_pipeline.py`（9 項）
1. LLM 選擇只保留候選集中合法項
2. LLM 選擇為空時回退 Step C 命中
3. Step B filter 字段正規化為 canonical field
4. 時間區間使用 dataset time dimension
5. invalid canonical 返回 `INVALID_CANONICAL_REF`
6. SQL compiler 以 canonical filter 穩定生成 SQL
7. Step D2 時間區間值不得被錯誤覆寫
8. 支援 YAML `True` key 的 join `on` 兼容解析
9. LLM 無效 filter 欄位在 D2 被清洗移除

#### C. `tests/test_main_diagnostics.py`（4 項）
1. 可生成資料時間邊界 SQL（MIN/MAX）
2. 請求時間與資料時間不相交時可回退到資料區間
3. 可替換 plan 中 between 時間條件
4. 空結果提示文案包含診斷與自動修正資訊

---

## 4) 驗證完成標準

- 端到端樣例 E2E-01 ~ E2E-08 至少人工跑過 1 次。
- 單元測試 16 項全綠。
- 圖表檔案存在：`artifacts/charts/query_chart.png`（或 `CHART_OUTPUT_DIR` 指定路徑）。
