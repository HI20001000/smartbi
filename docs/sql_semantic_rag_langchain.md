# SQL 任務：用 LangChain + RAG 檢索 semantics 層（指標 / 欄位 / 表）

這份設計對應你現在的流程：
1. 先把使用者輸入判斷成 SQL 任務。
2. 再從 semantic layer 找出最可能的 metric / field / dataset。
3. 最後把「候選語意物件」交給 SQL 生成器，避免 LLM 自由發揮。

## 一、建議的整體流程

- Step A：`Intent Router` 判斷是否 SQL。
- Step B：`Query Parser` 把句子拆成：
  - 目標指標（例如：存款餘額、客戶數）
  - 維度（例如：分行、產品、年月）
  - 條件（例如：2024Q4、澳門半島）
- Step C：`Semantic Retriever (RAG)` 在語意層做召回（recall）+ 重排（rerank）。
- Step D：`Semantic Resolver` 做規則校驗：
  - 敏感欄位是否禁用
  - join 路徑是否存在
  - 時間粒度是否可用
- Step E：`SQL Planner/Generator` 只根據通過校驗的語意物件生成 SQL。

## 二、語意層要先「文件化」才能 RAG

把 YAML 的 metric / field / dataset 展平成可檢索文件，每個文件保留 metadata。

每筆文件建議欄位：
- `object_type`: `metric | dimension | field | dataset | entity`
- `name`
- `aliases/synonyms`
- `description`
- `dataset`
- `table`
- `expr`
- `allowed`（敏感欄位可直接標 `false`）

例如（概念）：
- `metric: deposit_balance_daily.total_balance`
- `dimension: branch.region`
- `field: account.currency`
- `dataset: deposit_balance_daily`

## 三、LangChain 實作骨架（可直接套）

```python
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS


def build_docs_from_semantic(semantic_dict) -> list[Document]:
    docs = []

    # 1) entities / fields
    for entity_name, entity in semantic_dict["semantic_layer"].get("entities", {}).items():
        table = entity.get("table")
        for f in entity.get("fields", []):
            text = (
                f"entity={entity_name}\n"
                f"field={f.get('name')}\n"
                f"synonyms={','.join(f.get('synonyms', []))}\n"
                f"type={f.get('type')}\n"
                f"expr={f.get('expr')}\n"
            )
            docs.append(Document(
                page_content=text,
                metadata={
                    "object_type": "field",
                    "entity": entity_name,
                    "table": table,
                    "name": f.get("name"),
                    "allowed": True,
                },
            ))

        for sf in entity.get("sensitive_fields", []):
            text = (
                f"entity={entity_name}\n"
                f"field={sf.get('name')}\n"
                f"classification={sf.get('classification')}\n"
                f"expr={sf.get('expr')}\n"
            )
            docs.append(Document(
                page_content=text,
                metadata={
                    "object_type": "field",
                    "entity": entity_name,
                    "table": table,
                    "name": sf.get("name"),
                    "allowed": sf.get("allowed", False),
                    "sensitive": True,
                },
            ))

    # 2) datasets / metrics / dimensions
    for ds_name, ds in semantic_dict["semantic_layer"].get("datasets", {}).items():
        docs.append(Document(
            page_content=f"dataset={ds_name}\ndescription={ds.get('description', '')}",
            metadata={"object_type": "dataset", "name": ds_name, "allowed": True},
        ))

        for metric in ds.get("metrics", []):
            docs.append(Document(
                page_content=(
                    f"dataset={ds_name}\nmetric={metric.get('name')}\n"
                    f"synonyms={','.join(metric.get('synonyms', []))}\n"
                    f"description={metric.get('description', '')}\n"
                    f"expr={metric.get('expr')}\n"
                ),
                metadata={
                    "object_type": "metric",
                    "dataset": ds_name,
                    "name": metric.get("name"),
                    "allowed": True,
                },
            ))

    return docs


def build_retriever(docs: list[Document]):
    embeddings = OpenAIEmbeddings(model="text-embedding-3-large")
    store = FAISS.from_documents(docs, embeddings)
    return store.as_retriever(search_kwargs={"k": 20})
```

## 四、查詢時的「兩段檢索」

建議不要只做一次向量召回，準確率會不穩：

1. **第一段（召回）**
   - 用使用者原句 + 拆解後 keyword（指標詞、維度詞、時間詞）去檢索 top-k。
2. **第二段（重排）**
   - 用 LLM 對 top-k 做 rerank，輸出最終候選：
     - `target_metrics[]`
     - `target_dimensions[]`
     - `candidate_datasets[]`
     - `confidence`

重排 prompt 要求：
- 只能從候選文件選，不可杜撰。
- 若不確定，回傳 `need_clarification=true`。

## 五、關鍵防呆（非常重要）

在輸出 SQL 前，一定做語意治理檢查：

- 任何 `allowed=false` 欄位直接拒絕。
- metric 與 dimension 必須存在共同 dataset 或合法 join 路徑。
- 若語句無時間條件但 governance 要求時間過濾，強制追問。
- 同名欄位（例如多表都有 `status`）必須 disambiguation。

## 六、你這個專案的落地建議

你目前 `main.py` 在 SQL intent 只做提示，下一步可改成：

- 在啟動時載入 `app/semantics/smartbi_demo_macau_banking_semantic.yaml`。
- 建立 `SemanticRetriever`（可放 `app/semantic_retriever.py`）。
- 在 `IntentType.SQL` 時：
  1. 先跑 retriever
  2. 把 top 候選列給 LLM 做 schema-grounded SQL planning
  3. 回傳「我判斷你要的指標/欄位/表」給使用者確認

建議先做「可解釋輸出」格式，例如：

```json
{
  "metrics": ["ending_balance"],
  "dimensions": ["branch.region", "calendar.yyyy_mm"],
  "dataset": "deposit_balance_daily",
  "filters": ["calendar.yyyy_mm between 2024-01 and 2024-12"],
  "need_clarification": false
}
```

這樣 SQL 生成才穩，且容易 debug。
