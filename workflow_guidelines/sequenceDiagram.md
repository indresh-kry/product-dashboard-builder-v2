sequenceDiagram
autonumber

%% === PARTICIPANTS ===
participant CS as Clickstream Data
participant ORC as Orchestrator Workflow
participant ENV as Environment Initialization
participant SM as Schema Mapper
participant U as User (Clarification)
participant RE as Rules Engine<br/>(Differential Rulesets)
participant DAGG as Data Aggregation
participant SQL as SQL Runner
participant MEM as Agent Memory
participant ANA as Data Analyst
participant LLM as LLM Workflow
participant DB as Database
participant ACT as Action

%% === LLM / ACTION / DB CONTEXT ===
ACT->>LLM: Trigger action/context
LLM->>DB: Read/Write (supporting metadata & prompts)
DB-->>LLM: Data/State

%% === CORE ORCHESTRATION ===
CS->>ORC: 1) Provide raw clickstream events
ORC->>ENV: 2) Initialize environment
ENV-->>ORC: Env ready

ORC->>SM: 3) Request identifier categories & taxonomy
SM->>RE: Fetch rules for identifiers
RE-->>SM: Ruleset returned

loop For each identifier
  SM->>SM: Infer field mapping (identifier ↔ field)
  alt Mapping found
    SM-->>ORC: 5) Mapping returned
  else Mapping missing
    SM-->>U: Ask for clarification
    U-->>SM: Clarification provided
    SM->>SM: Re-run inference
  end
end

ORC->>DAGG: 6) Provide output table specs & contracts
DAGG->>SQL: Generate SQL for aggregates & metrics

SQL->>MEM: 7) Create aggregate table
SQL->>MEM: 8) Populate aggregate table
SQL->>MEM: 9) Create per-metric output tables

ORC-->>ANA: 10) Pass product metric tables
ANA->>ANA: Timeseries analysis
ANA->>ANA: Identify data gaps
Note over ANA: Benchmark comparison (marked out of scope)