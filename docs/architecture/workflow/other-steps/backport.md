Datasets from our production grapher database can be backported to ETL catalog.

These is useful when a dataset was imported to Grapher not by using the ETL (years ago, or manually imported datasets through the admin) and we want to make it available in ETL.


```mermaid
flowchart LR

    garden1((____)):::node -->|format| grapher1((____)):::node
    grapher1((____)):::node -->|load| grapher2a((____)):::node
    grapher2b((____)):::node -.->|backport| garden1((____)):::node


    subgraph id3 [Garden]
    garden1
    end

    subgraph id4 [Grapher]
    grapher1
    end

    subgraph id5 [Grapher]
    grapher2a
    grapher2b
    end

    subgraph id [ETL]
    id3
    id4
    end

    classDef node fill:#002147,color:#002147
    classDef node_ss fill:#002147,color:#fff
```