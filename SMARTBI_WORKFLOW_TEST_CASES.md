# SmartBI 工作流驗證樣例（基於 `sql/exmaple_data.sql`）

> 目的：提供可直接執行的「輸入樣例 + 預期行為」，覆蓋目前 `tests/` 內所有測試樣例（語義規劃、診斷修正、圖表規劃）。

## 0) 測試資料準備

1. 建立/重建示例庫：

```bash
mysql -u <user> -p < sql/exmaple_data.sql
```

2. 設定環境變數（最小）

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

## 1) 端到端輸入樣例（Step A ~ Step I）

> 每個樣例都建議檢查輸出中的：
> - `Step C Token 命中結果`
> - `Step D2 合併後計畫（Deterministic）`
> - `Step E 規則校驗`
> - `Step F SQL 生成結果`
> - `Step G SQL 執行筆數`
> - `Step H 圖表規劃`
> - `Step I 圖表輸出`

### Case E2E-01：時間序列折線圖（line）
- **輸入**：`查詢 2024年1月 每日存款餘額趨勢`
- **預期**：
  - 含時間維度 `biz_date`
  - `chart_type='line'`
  - 有 `GROUP BY` 日期欄位

### Case E2E-02：類別維度條形圖（bar）
- **輸入**：`統計 2024年1月 各分行存款餘額`
- **預期**：
  - 維度命中 `branch.branch_name` 或 `branch.region`
  - `chart_type='bar'`

### Case E2E-03：數值-only 結果也要輸出條形圖（bar）
- **輸入**：`查詢 2024年1月 澳門半島存款餘額總額`
- **預期**：
  - 只有單一 metric 欄位時，不可退回 `table`
  - `Step H` 應為 `chart_type='bar'`
  - x 軸使用 row index（內部 key: `__row_index__`）

### Case E2E-04：區域過濾 + 時間區間
- **輸入**：`查詢 2024-01-01 到 2024-01-31 澳門半島存款餘額`
- **預期**：
  - `selected_filters` 應包含 `branch.region='澳門半島'`
  - `selected_filters` 應包含 `biz_date between ...`

### Case E2E-05：時間超界自動修正（空結果診斷）
- **輸入**：`查詢 2030年全年 澳門半島存款餘額`
- **預期**：
  - 若初次結果為空，出現診斷文字：`查詢時間範圍可能超出資料可用區間`
  - 出現自動修正提示：`已自動改用可用時間範圍重新查詢`

### Case E2E-06：安全與語義治理（無效/禁用欄位）
- **輸入**：`查詢客戶姓名與身份證號`
- **預期**：
  - 應被語義治理攔截或回覆不可查
  - 不得生成洩漏 PII 的 SQL

---

## 2) 覆蓋目前所有測試樣例的對照清單

> 下列命令可直接覆蓋 repo 目前測試樣例（含 chart planner / semantic pipeline / main diagnostics）。

```bash
python -m unittest tests/test_chart_planner.py tests/test_semantic_pipeline.py tests/test_main_diagnostics.py
```

### 測試樣例對照（15 項）

#### A. `tests/test_chart_planner.py`（3 項）
1. Decimal 指標應規劃為 bar
2. 第一列為空值、後續為數值時仍可識別數值欄
3. numeric-only 結果使用 row index 生成 bar

#### B. `tests/test_semantic_pipeline.py`（8 項）
1. LLM 選擇只保留候選集中合法項
2. LLM 選擇為空時回退 Step C 命中
3. Step B filter 字段正規化為 canonical field
4. 時間區間使用 dataset time dimension
5. invalid canonical 返回 `INVALID_CANONICAL_REF`
6. SQL compiler 以 canonical filter 穩定生成 SQL
7. Step D2 時間區間值不得被錯誤覆寫
8. 支援 YAML `True` key 的 join `on` 兼容解析

#### C. `tests/test_main_diagnostics.py`（4 項）
1. 可生成資料時間邊界 SQL（MIN/MAX）
2. 請求時間與資料時間不相交時可回退到資料區間
3. 可替換 plan 中 between 時間條件
4. 空結果提示文案包含診斷與自動修正資訊

---

## 3) 建議驗證完成標準

- 端到端樣例 E2E-01 ~ E2E-06 至少人工跑過 1 次。
- 單元測試 15 項全綠。
- 圖表輸出檔案存在：`artifacts/charts/query_chart.png`（或 `CHART_OUTPUT_DIR` 指定路徑）。
