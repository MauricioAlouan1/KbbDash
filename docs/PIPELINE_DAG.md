graph TD
    XML_NFI[XML NFI Files]
    XML_NF[XML NF Files]
    
    Step1_NFI[Step 1: NFI_1_Create]
    Step1_NF[Step 1: NF_1_Create]
    
    L1_NFI[Level 1 NFI Excel]
    L1_NF[Level 1 NF Excel]
    
    Step2_NFI[Step 2: NFI_2_Aggregate]
    Step2_NF[Step 2: NF_2_Aggregate]
    
    Todos_NFI[NFI_..._todos.xlsm]
    Todos_NF[NF_..._todos.xlsm]
    
    Step2_5[Step 2.5: process_data.py]
    Raw_Data[Raw Data Files]
    
    subgraph Clean_Data_Groups [Clean Data Groups]
        direction TB
        Inv_Files[Inventory Files<br/>B_Estoq, T_EstTrans, O_Estoq<br/>B_EFull...]
        Other_Files[Other Clean Files<br/>O_NFCI, O_CC, L_LPI<br/>KON_RelGeral, MLK_ExtLib...]
    end
    
    Step3[Step 3: Atualiza_Entradas]
    T_Entradas[T_Entradas.xlsx]
    
    Manual[MANUAL STEP: Open/Save T_Entradas]
    
    Step4[Step 4: Inventory Process]
    R_Estoq[R_Estoq_fdm.xlsx]
    
    Step5[Step 5: Report]
    R_Resumo[R_Resumo.xlsx]
    
    Conc_Estoque[Conc_Estoque.py]
    Conc_CAR[Conc_CARReceber_v2.py]
    Compras[compras.py]
    
    XML_NFI --> Step1_NFI
    Step1_NFI --> L1_NFI
    
    XML_NF --> Step1_NF
    Step1_NF --> L1_NF
    
    L1_NFI --> Step2_NFI
    Step2_NFI --> Todos_NFI
    
    L1_NF --> Step2_NF
    Step2_NF --> Todos_NF
    
    Raw_Data --> Step2_5
    Step2_5 --> Inv_Files
    Step2_5 --> Other_Files
    
    Todos_NFI --> Step3
    Todos_NF --> Step3
    Step3 --> T_Entradas
    
    T_Entradas --> Manual
    Manual --> Step4
    Inv_Files --> Step4
    Step4 --> R_Estoq
    
    R_Estoq --> Step5
    T_Entradas --> Step5
    Other_Files --> Step5
    Step5 --> R_Resumo
    
    R_Resumo --> Conc_Estoque
    R_Resumo --> Conc_CAR
    R_Resumo --> Compras
    
    style Step1_NFI fill:#f9f,stroke:#333,stroke-width:2px
    style Step1_NF fill:#f9f,stroke:#333,stroke-width:2px
    style Step2_NFI fill:#f9f,stroke:#333,stroke-width:2px
    style Step2_NF fill:#f9f,stroke:#333,stroke-width:2px
    style Step2_5 fill:#f9f,stroke:#333,stroke-width:2px
    style Step3 fill:#f9f,stroke:#333,stroke-width:2px
    style Step4 fill:#f9f,stroke:#333,stroke-width:2px
    style Step5 fill:#f9f,stroke:#333,stroke-width:2px
    style Conc_Estoque fill:#f9f,stroke:#333,stroke-width:2px
    style Conc_CAR fill:#f9f,stroke:#333,stroke-width:2px
    style Compras fill:#f9f,stroke:#333,stroke-width:2px
    style Manual fill:#ff9,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5
