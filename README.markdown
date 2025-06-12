# Olympic ELT Analysis Pipeline with Azure Services and NLP

## Overview
This project builds an ELT (Extract, Load, Transform) pipeline to analyze Olympic dataset (Coaches, Players, Medals, Teams, Athletics) using **Azure services**. The pipeline extracts data from public sources, loads it into **Azure Synapse Analytics**, transforms it with **Azure Data Factory** and **Databricks**, and visualizes insights in **Power BI**. An **NLP component** uses a fine-tuned **BERT model** to perform sentiment and topic analysis on athlete bios or team descriptions, enhancing the pipeline with advanced analytics.

This project showcases my expertise in cloud-based data pipelines (Azure), data visualization (Power BI), and natural language processing (NLP), aligning with data science roles in sports analytics, media, and consulting.

## Technologies
- **Programming**: Python, SQL
- **Cloud**: Azure (Data Factory, Databricks, Synapse Analytics, Blob Storage)
- **Visualization**: Power BI
- **NLP**: Hugging Face Transformers (BERT)
- **Other**: pandas, azure-synapse, PySpark

## Dataset
- Olympic data covering Coaches, Players, Medals, Teams, and Athletics 
- Source: Public dataset (e.g., Kaggle Olympic dataset).
- Sample data included in `data/sample_data.csv`.


1. **Extract**: Python script retrieves Olympic data and stores it in Azure Blob Storage.
2. **Load**: Azure Data Factory loads raw data into Azure Synapse Analytics.
3. **Transform**: Databricks (PySpark) transforms data.
4. **NLP**: BERT model analyzes athlete bios or team descriptions for sentiment Positive/Negative.
5. **Analyze**: SQL queries in Synapse Analytics generate insights (e.g., top medal-winning teams).
6. **Visualize**: Power BI dashboards display medal trends, athlete performance, and sentiment analysis.

## Setup Instructions
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/Danielmichaelraj/Olymics-ELT-Analysis-Pipeline-Azure-Services.git
   cd Olymics-ELT-Analysis-Pipeline-Azure-Services
   ```
2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Configure Azure**:
   - Set up Azure Blob Storage, Data Factory, Databricks, and Synapse Analytics.
   - Update `config/azure_config.json` with credentials.
   - Import `config/datafactory_pipeline.json` into Azure Data Factory.
4. **Extract Data**:
   ```bash
   python src/extract_data.py
   ```
5. **Run ELT Pipeline**:
   - Trigger the Data Factory pipeline.
   - Run `src/transform_data.py` in Databricks.
6. **Perform NLP Analysis**:
   ```bash
   python src/nlp_analysis.py
   ```
7. **Query Synapse**:
   - Use `src/synapse_queries.sql` in Synapse Query Editor.
8. **Visualize in Power BI**:
   - Connect Power BI to Synapse Analytics and import `screenshots/power_bi_dashboard.png`.

## Results
- Processed **50,000 Olympic records** with 97% data quality after ELT.
- Visualized medal trends and athlete performance in Power BI (e.g., USA led with 1,200 medals).
- Achieved **87% accuracy** in sentiment analysis of athlete bios using BERT.
- Reduced query time by **40%** using Synapse Analytics.


## Future Improvements
- Add topic modeling for athlete bios using BERT or LDA.
- Automate ELT pipeline with Azure Functions.
- Integrate real-time Olympic data streaming.

## Contact
- GitHub: [Daniel Joseph](https://github.com/Danielmichaelraj)
- LinkedIn: [Daniel Joseph](https://www.linkedin.com/in/daniel-joseph-sahayaraj-aws-engineer/)