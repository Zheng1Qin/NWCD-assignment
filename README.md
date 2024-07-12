# NWCD-assignment

---

# NWCD Assignment – Data Engineer

## 场景描述：

Q公司销售部有4个系统，分别是 CRM system, Billing system, Data Platform, BI system；

### 1. CRM 中有四张表：

#### a) 表 Opportunity，包含关键字段：

- OpportunityID
- OpportunityName
- AccountID
- ForecastAmount
- Stage
- CreateDate
- LaunchDate

#### b) 表 Activity，包含关键字段：

- ActivityID
- ActivityName
- ActivityType
- AccountID
- OpportunityID

#### c) 表 Account，包含关键字段：

- AccountID
- AccountName
- OwnerTerritoryID
- ActiveFlag
- ForecastAmount

#### d) 表 Owner ，包含关键字段：

- OwnerTerritoryID
- OwnerName
- OwnerType
- OwnerTeam
- OwnerManager

### 2. Billing 中有 CSV 格式的文件，包含关键字段：

- AccountID
- SubAccountID
- OwnerTerritoryID
- ProductName
- ChargeType
- RevenueAmount
- ChargeDate

### 3. Data Platform 中包含 ETL、workflow、数据仓库等工具；

### 4. BI System 中包含数据连接，数据可视化工具；

## 需求说明：

需要做一个看板：展示本财年，每周的实际收入，每周的预测收入，以及周度的预测收入变化；

## 要求：

1. 请提交一份不超过6页的文件，格式为PDF；
2. 请基于你的理解进行需求分析；
3. 请提供完整的方案，并详细说明方案中各种选择的原因；
4. 如有必要，请提供关键步骤的处理代码，比如SQL、Python等；
5. 请说明设计中的所有假设和引用。

---
