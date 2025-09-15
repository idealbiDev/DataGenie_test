import random

def getAdvert():
    ad_templates = [
    {
        "title": "MDM Assistant: Precision Master Data Management",
        "description": "Leverage AI-driven fuzzy matching and natural language processing to cleanse, audit, and explore critical master data. Achieve unparalleled data integrity and streamline operations with intelligent query generation.",
        "link": "https://yourdomain.com/mdm/query",
        "cta": "Optimize Master Data"
    },
    {
        "title": "Data Dictionary Generator: Automated Data Documentation",
        "description": "Effortlessly create comprehensive data dictionaries from any database. Automate schema and table Browse, and generate AI-powered Markdown reports with detailed column metadata for enhanced data governance.",
        "link": "https://yourdomain.com/dictionary/dictionary",
        "cta": "Generate Data Dictionary"
    },
    {
        "title": "Data Quality Monitor: Proactive Data Integrity",
        "description": "Implement continuous data quality surveillance. Define robust rules, automatically detect anomalies and inconsistencies, and receive actionable alerts to maintain the pristine integrity of your critical datasets over time.",
        "link": "https://yourdomain.com/datamonitor/datamonitor",
        "cta": "Monitor Data Quality"
    },
    {
        "title": "ISO Certification: Streamlined Compliance for Data & Quality",
        "description": "Achieve and maintain ISO 8000 & ISO 25500 compliance with an intuitive platform. Establish robust, certified Quality Management Systems (QMS) and data processes, ensuring regulatory adherence and operational excellence.",
        "link": "https://yourdomain.com/isostandards/isostandards",
        "cta": "Attain ISO Compliance"
    },
    {
        "title": "Materiq: ISO 8000 Material Master Governance",
        "description": "Revolutionize material master data with ISO 8000-aligned governance and cleansing. Eliminate inventory inefficiencies through ISD dictionary support, vendor standardization, and AI-powered duplicate detection and workflow automation.",
        "link": "https://yourdomain.com/materiq/",
        "cta": "Enhance Material Data"
    },
    {
        "title": "Data Privacy Guard: Automated PII & Sensitive Data Discovery",
        "description": "Automate discovery and classification of PII across your databases. Leverage AI to identify data types, enforce stringent privacy policies, and generate compliance reports for GDPR, POPIA, and CCPA.",
        "link": "https://yourdomain.com/privacy/discover",
        "cta": "Secure Data Privacy"
    },
    {
        "title": "Schema Harmonizer: Unify Disparate Data Schemas",
        "description": "Intelligently consolidate and harmonize fragmented database schemas. AI identifies common entities, suggests optimal mapping rules, and automates complex data integration, significantly reducing manual effort.",
        "link": "https://yourdomain.com/schema-harmonizer/unify",
        "cta": "Harmonize Schemas"
    },
    {
        "title": "Lead Verification: AI-Powered Lead Qualification",
        "description": "Refine your marketing funnel with AI-driven lead filtering and analysis. Submit natural language queries or apply demographic filters. Benefit from LLM-generated database and precise lead segmentation for optimized campaigns.",
        "link": "https://yourdomain.com/leads/leads",
        "cta": "Verify Leads Now"
    },
    {
        "title": "HourWise: Intuitive Time Tracking & Productivity",
        "description": "Simplify work hour management for individuals and teams. Effortlessly log time spent on projects, tasks, and clients to ensure accurate records for billing, payroll, and insightful productivity analysis.",
        "link": "https://yourdomain.com/hourwise/timesheet",
        "cta": "Track Time Smartly"
    },
    {
        "title": "Contract Analyzer: AI-Driven Legal Document Review",
        "description": "Streamline legal reviews with intelligent contract analysis. AI extracts key clauses, identifies risks, summarizes terms, and compares versions, ensuring compliance and efficiency without extensive manual reading.",
        "link": "https://yourdomain.com/contracts/analyze",
        "cta": "Analyze Contracts"
    },
    {
        "title": "Sentiment Surveyor: Actionable Customer Feedback Insights",
        "description": "Extract sentiment and key themes from customer feedback, reviews, and surveys. Utilize AI to gain actionable insights into customer satisfaction, product performance, and market trends for strategic decision-making.",
        "link": "https://yourdomain.com/feedback/analyze",
        "cta": "Survey Sentiment"
    },
    {
        "title": "Smart Meeting Summarizer: Enhance Meeting Efficiency",
        "description": "Automate meeting transcription and summarization. AI identifies key decisions, action items, and discussion points, delivering concise summaries and follow-up tasks to boost meeting productivity and accountability.",
        "link": "https://yourdomain.com/meetings/summarize",
        "cta": "Summarize Meetings"
    },
    {
        "title": "Resource Allocator: Optimized Team & Project Resourcing",
        "description": "Optimize resource allocation across teams and projects. AI suggests optimal task assignments based on skills, availability (integrated with HourWise), and deadlines, preventing burnout and maximizing output.",
        "link": "https://yourdomain.com/resources/allocate",
        "cta": "Allocate Resources"
    }

    ,
    {
        "title": "WorkingPapers: Automate Financial Audit Readiness",
        "description": "Streamline financial working paper preparation and review. AI-driven discrepancy identification ensures compliance and accelerates audit readiness, boosting accuracy for finance professionals.",
        "link": "https://yourdomain.com/fintech/workingpapers",
        "cta": "Automate Audits"
    },
    {
        "title": "Patient Data Harmonizer: Unified Healthcare Records",
        "description": "Integrate and normalize patient data from disparate sources. AI-powered cleaning and deduplication create accurate, unified patient records, improving diagnostics, treatment, and HIPAA compliance.",
        "link": "https://yourdomain.com/healthcare/patientdata",
        "cta": "Unify Patient Data"
    },
    {
        "title": "CropSense AI: Precision Crop Disease Detection",
        "description": "Early and accurate AI-powered detection of crop diseases. Image recognition and machine learning analyze plant health, providing timely alerts and recommendations to minimize crop loss and optimize yields.",
        "link": "https://yourdomain.com/agritech/cropdisease",
        "cta": "Diagnose Crops with AI"
    },
    {
        "title": "DataFlow Forge: Build Robust Data Pipelines",
        "description": "Design, build, and manage efficient data pipelines for ingestion, transformation, and loading. Seamlessly integrate with diverse data sources, ensuring data readiness for analytics and reporting.",
        "link": "https://yourdomain.com/dataeng/pipelines",
        "cta": "Forge Data Pipelines"
    },
    {
        "title": "InsightReport Pro: Automated Business Reporting",
        "description": "Automate the generation and distribution of critical business reports. Connect to diverse data sources, define reporting logic, and schedule automated delivery for timely, consistent access to performance metrics.",
        "link": "https://yourdomain.com/dataeng/reporting",
        "cta": "Automate Reports"
    },
    {
        "title": "VisuaGraph: Transform Data into Insightful Visuals",
        "description": "Create interactive data visualizations and dashboards with tools like Power BI and Microsoft Fabric. Convert complex datasets into intuitive graphics for deeper analysis and informed decision-making.",
        "link": "https://yourdomain.com/dataeng/visualizations",
        "cta": "Visualize Your Data"
    }

]
    


    ad=   random.randint(0, len(ad_templates) - 1)
    ad_content= ad_templates[ad]
 

    return ad_content

def getSolution():
    solutions = [
        {
            "group_name": "Data Management & Intelligence",
            "description": "Tools designed to help you organize, clean, integrate, and explore your critical business data, ensuring accuracy and accessibility.",
            "solutions": [
                {
                    "name": "mdm",
                    "display": "MDM Assistant",
                    "url": "/mdm/query",
                    "icon": "mdmlogo.png",
                    "description": (
                        "A smart assistant for master data management. "
                        "Use fuzzy matching and natural language to clean, audit, and explore data in a Database. "
                        "Supports OpenAI and Cohere for intelligent query generation."
                    )
                },
                {
                    "name": "dictionary",
                    "display": "Data Dictionary Generator",
                    "url": "/dictionary/dictionary",
                    "icon": "ddglogo.png",
                    "description": (
                        "Automatically create data dictionaries from any database. "
                        "Connect, browse schemas and tables, and generate Markdown reports with column metadata using AI."
                    )
                },
                {
                    "name": "quality_flow",
                    "display": "Data Quality Monitor",
                    "url": "/datamonitor/datamonitor",
                    "icon": "data_quality_monitor.png",
                    "description": (
                        "Continuously monitor the quality of your database data. "
                        "Define data quality rules, automatically detect anomalies, inconsistencies, and errors, "
                        "and generate actionable insights and alerts to maintain data integrity over time."
                    )
                },
                {
                    "name": "isostandards",
                    "display": "ISO Certification",
                    "url": "/isostandards/isostandards",
                    "icon": "merge-cells.png",
                    "description": (
                        "Provides comprehensive support for achieving and maintaining Quality Management Systems (QMS) implementation and certification. specializes in helping you align with ISO 8000  and ISO 25500  standards, empowering your organization to establish robust, certified, and efficient data and quality processes through an intuitive platform. "
                    )
                },
                {
                    "name": "materiq",
                    "display": "Materiq",
                    "url": "/materiq/",
                    "icon": "materiq_logo.png",
                    "description": (
                        "ISO 8000-aligned material master data governance and cleansing engine. "
                        "Features built-in ISD dictionary support, vendor standardization, duplicate detection, and governance workflows. "
                        "Helps organizations eliminate inventory inefficiencies by improving the quality and structure of their material records."
                    )
                },
                {
                    "name": "data_privacy_guard",
                    "display": "Data Privacy Guard",
                    "url": "/privacy/discover",
                    "icon": "data_privacy_guard.png",
                    "description": (
                        "Automated discovery and classification of Personally Identifiable Information (PII) and sensitive data across your databases. "
                        "Leverage AI to identify data types, enforce privacy policies, and generate compliance reports for regulations like GDPR, POPIA, or CCPA."
                    )
                },
                {
                    "name": "schema_harmonizer",
                    "display": "Schema Harmonizer",
                    "url": "/schema-harmonizer/unify",
                    "icon": "schema_harmonizer.png",
                    "description": (
                        "Intelligently harmonize and consolidate disparate database schemas. "
                        "Use AI to identify common entities, suggest mapping rules, and automate data integration processes, "
                        "reducing manual effort in complex data migration and consolidation projects."
                    )
                }
            ]
        },
        {
            "group_name": "Business & Workflow Optimization",
            "description": "AI-powered tools to enhance efficiency, streamline operations, and gain actionable insights across various business functions.",
            "solutions": [
                {
                    "name": "leads",
                    "display": "Lead Verification",
                    "url": "/leads/leads",
                    "icon": "ldvlogo.png",
                    "description": (
                        "Filter and analyze marketing leads using AI. "
                        "Submit plain-language queries or choose from demographic filters. "
                        "LLM-generated Database and lead segmentation included."
                    )
                },
                {
                    "name": "hourwise",
                    "display": "HourWise",
                    "url": "/hourwise/timesheet",
                    "icon": "hwtlogo.png",
                    "description": (
                        "HourWise is an intuitive and powerful time tracking application designed to simplify how individuals and teams manage their work hours. With HourWise, you can effortlessly log time spent on projects, tasks, and clients, ensuring accurate records for billing, payroll, and productivity analysis."
                    )
                },
                {
                    "name": "legal_lens",
                    "display": "Contract Analyzer",
                    "url": "/contracts/analyze",
                    "icon": "file-contract.png",
                    "description": (
                        "Intelligently analyze contracts and legal documents using AI. "
                        "Extract key clauses, identify risks, summarize terms, and compare versions "
                        "to streamline legal reviews and ensure compliance without extensive manual reading."
                    )
                },
                {
                    "name": "feedback_insight",
                    "display": "Sentiment Surveyor",
                    "url": "/feedback/analyze",
                    "icon": "face-smile.png",
                    "description": (
                        "Analyze customer feedback, reviews, and survey responses to extract sentiment and key themes. "
                        "Utilize AI to gain actionable insights into customer satisfaction, product performance, and market trends."
                    )
                },
                {
                    "name": "meetnotes_ai",
                    "display": "Smart Meeting Summarizer",
                    "url": "/meetings/summarize",
                    "icon": "microphone-alt.png",
                    "description": (
                        "Automatically transcribe and summarize meeting discussions. "
                        "AI identifies key decisions, action items, and discussion points, "
                        "providing concise summaries and follow-up tasks to improve meeting efficiency and accountability."
                    )
                },
                {
                    "name": "teamflow_pro",
                    "display": "Resource Allocator",
                    "url": "/resources/allocate",
                    "icon": "users-gear.png",
                    "description": (
                        "Optimize team and project resource allocation. "
                        "Use AI to suggest optimal task assignments based on team skills, availability (integrated with HourWise data), "
                        "and project deadlines, preventing burnout and maximizing output."
                    )
                }
            ]
        },
        {
            "group_name": "FinTech Solutions",
            "description": "Innovative financial technology solutions leveraging AI and data management for enhanced efficiency, compliance, and decision-making in the financial sector.",
            "solutions": [
                {
                    "name": "working_papers",
                    "display": "WorkingPapers",
                    "url": "/fintech/workingpapers",
                    "icon": "fintech_icon.png",  # Placeholder icon
                    "description": (
                        "Automate and streamline the preparation and review of financial working papers. "
                        "Utilize AI to identify discrepancies, ensure compliance with accounting standards, "
                        "and accelerate audit readiness, enhancing accuracy and reducing manual effort for financial professionals."
                    )
                }
            ]
        },
        {
            "group_name": "HealthCare Innovations",
            "description": "Cutting-edge solutions for the healthcare industry, focusing on data-driven insights, operational efficiency, and improved patient outcomes through advanced technology.",
            "solutions": [
                {
                    "name": "patient_data_harmonizer",
                    "display": "Patient Data Harmonizer",
                    "url": "/healthcare/patientdata",
                    "icon": "healthcare_icon.png",  # Placeholder icon
                    "description": (
                        "Integrate and normalize patient data from disparate sources, ensuring a unified and accurate patient record. "
                        "Leverage AI for data cleaning, deduplication, and standardization, supporting better diagnostics, "
                        "treatment planning, and regulatory compliance (e.g., HIPAA)."
                    )
                }
            ]
        },
        {
            "group_name": "AgriTech Advancements",
            "description": "Technological solutions revolutionizing agriculture, enabling smart farming practices, optimizing crop management, and ensuring sustainable food production through AI and data analytics.",
            "solutions": [
                {
                    "name": "crop_diagnose_ai",
                    "display": "CropSense AI",
                    "url": "/agritech/cropdisease",
                    "icon": "agritech_icon.png",  # Placeholder icon
                    "description": (
                        "AI-powered solution for early and accurate detection of crop diseases. "
                        "Utilizes image recognition and machine learning to analyze plant health, "
                        "providing timely alerts and recommendations to minimize crop loss and optimize yields."
                    )
                }
            ]
        },
        {
            "group_name": "Data Engineering & Business Intelligence",
            "description": "Comprehensive solutions for data pipeline development, reporting, and interactive visualizations, leveraging leading industry tools to transform raw data into actionable insights.",
            "solutions": [
                {
                    "name": "data_pipeline_builder",
                    "display": "DataFlow Forge",
                    "url": "/dataeng/pipelines",
                    "icon": "data_pipeline_icon.png",  # Placeholder icon
                    "description": (
                        "Design, build, and manage robust data pipelines for efficient data ingestion, transformation, and loading. "
                        "Integrate seamlessly with various data sources and destinations, ensuring data readiness for analytics and reporting."
                    )
                },
                {
                    "name": "report_automator",
                    "display": "InsightReport Pro",
                    "url": "/dataeng/reporting",
                    "icon": "reporting_icon.png",  # Placeholder icon
                    "description": (
                        "Automate the generation and distribution of critical business reports. "
                        "Connect to diverse data sources, define reporting logic, and schedule automated delivery "
                        "to key stakeholders, ensuring timely and consistent access to performance metrics."
                    )
                },
                {
                    "name": "visual_dashboard_creator",
                    "display": "VisuaGraph",
                    "url": "/dataeng/visualizations",
                    "icon": "visualization_icon.png",  # Placeholder icon
                    "description": (
                        "Create interactive and insightful data visualizations and dashboards using tools like Power BI and Microsoft Fabric. "
                        "Transform complex datasets into intuitive graphical representations, enabling deeper analysis and informed decision-making."
                    )
                }
            ]
        }
    ]
    return solutions