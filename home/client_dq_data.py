def getdata_qualityResults():
    # Sample data structure - expanded with more datasets, tables, and columns
    dqData = [
        {
            "name": "Sales",
            "tables": [
                {
                    "name": "Customers",
                    "quality": {"good": 78, "bad": 15, "critical": 7},
                    "columns": [
                        {
                            "name": "customer_id",
                            "type": "integer",
                            "metrics": [
                                {"name": "Completeness", "value": 100, "status": "good"},
                                {"name": "Uniqueness", "value": 100, "status": "good"},
                                {"name": "Validity", "value": 100, "status": "good"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "Sequential integers"},
                                {"name": "Min value", "value": 1},
                                {"name": "Max value", "value": 12548}
                            ],
                            "recommendations": [
                                "No issues detected",
                                "Consider partitioning for large datasets"
                            ]
                        },
                        {
                            "name": "email",
                            "type": "varchar(100)",
                            "metrics": [
                                {"name": "Completeness", "value": 92, "status": "good"},
                                {"name": "Format", "value": 72, "status": "critical"},
                                {"name": "Uniqueness", "value": 95, "status": "bad"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "firstname.lastname@domain.com"},
                                {"name": "Invalid patterns", "value": "1234 (9.8%)"}
                            ],
                            "recommendations": [
                                "Implement email validation at data entry",
                                "Clean existing invalid emails",
                                "Check for duplicate emails"
                            ]
                        },
                        {
                            "name": "phone",
                            "type": "varchar(20)",
                            "metrics": [
                                {"name": "Completeness", "value": 85, "status": "bad"},
                                {"name": "Format", "value": 88, "status": "good"},
                                {"name": "Accuracy", "value": 90, "status": "good"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "+1-XXX-XXX-XXXX"},
                                {"name": "Invalid patterns", "value": "Non-standard formats (7.2%)"}
                            ],
                            "recommendations": [
                                "Standardize phone number formats",
                                "Fill missing phone numbers where possible"
                            ]
                        }
                    ]
                },
                {
                    "name": "Orders",
                    "quality": {"good": 85, "bad": 10, "critical": 5},
                    "columns": [
                        {
                            "name": "order_id",
                            "type": "integer",
                            "metrics": [
                                {"name": "Completeness", "value": 100, "status": "good"},
                                {"name": "Uniqueness", "value": 100, "status": "good"},
                                {"name": "Validity", "value": 99, "status": "good"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "Sequential integers"},
                                {"name": "Min value", "value": 1000},
                                {"name": "Max value", "value": 98765}
                            ],
                            "recommendations": [
                                "No major issues detected",
                                "Monitor for gaps in sequence"
                            ]
                        },
                        {
                            "name": "order_date",
                            "type": "datetime",
                            "metrics": [
                                {"name": "Completeness", "value": 98, "status": "good"},
                                {"name": "Format", "value": 95, "status": "bad"},
                                {"name": "Timeliness", "value": 90, "status": "good"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "YYYY-MM-DD HH:MM:SS"},
                                {"name": "Invalid patterns", "value": "Non-standard formats (4.5%)"}
                            ],
                            "recommendations": [
                                "Enforce consistent date format",
                                "Validate historical data"
                            ]
                        },
                        {
                            "name": "total_amount",
                            "type": "decimal(10,2)",
                            "metrics": [
                                {"name": "Completeness", "value": 99, "status": "good"},
                                {"name": "Accuracy", "value": 92, "status": "bad"},
                                {"name": "Range", "value": 100, "status": "good"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "Positive decimals"},
                                {"name": "Min value", "value": 0.01},
                                {"name": "Max value", "value": 5000.00}
                            ],
                            "recommendations": [
                                "Verify negative or zero amounts",
                                "Implement range checks"
                            ]
                        }
                    ]
                },
                {
                    "name": "Returns",
                    "quality": {"good": 90, "bad": 8, "critical": 2},
                    "columns": [
                        {
                            "name": "return_id",
                            "type": "integer",
                            "metrics": [
                                {"name": "Completeness", "value": 100, "status": "good"},
                                {"name": "Uniqueness", "value": 100, "status": "good"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "Sequential integers"},
                                {"name": "Min value", "value": 1},
                                {"name": "Max value", "value": 5432}
                            ],
                            "recommendations": [
                                "No issues detected"
                            ]
                        },
                        {
                            "name": "reason",
                            "type": "varchar(200)",
                            "metrics": [
                                {"name": "Completeness", "value": 80, "status": "bad"},
                                {"name": "Consistency", "value": 85, "status": "bad"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "Text descriptions"},
                                {"name": "Common values", "value": "'Defective', 'Wrong item'"}
                            ],
                            "recommendations": [
                                "Standardize return reason categories",
                                "Fill missing reasons"
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "name": "Inventory",
            "tables": [
                {
                    "name": "Products",
                    "quality": {"good": 82, "bad": 12, "critical": 6},
                    "columns": [
                        {
                            "name": "product_id",
                            "type": "integer",
                            "metrics": [
                                {"name": "Completeness", "value": 100, "status": "good"},
                                {"name": "Uniqueness", "value": 100, "status": "good"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "Sequential integers"},
                                {"name": "Min value", "value": 100},
                                {"name": "Max value", "value": 9876}
                            ],
                            "recommendations": [
                                "No issues detected"
                            ]
                        },
                        {
                            "name": "product_name",
                            "type": "varchar(150)",
                            "metrics": [
                                {"name": "Completeness", "value": 95, "status": "good"},
                                {"name": "Uniqueness", "value": 90, "status": "bad"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "Alphanumeric names"},
                                {"name": "Duplicate count", "value": "5 duplicates found"}
                            ],
                            "recommendations": [
                                "Resolve duplicate product names",
                                "Ensure unique identifiers"
                            ]
                        },
                        {
                            "name": "stock_quantity",
                            "type": "integer",
                            "metrics": [
                                {"name": "Completeness", "value": 98, "status": "good"},
                                {"name": "Accuracy", "value": 85, "status": "bad"},
                                {"name": "Range", "value": 100, "status": "good"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "Positive integers"},
                                {"name": "Min value", "value": 0},
                                {"name": "Max value", "value": 10000}
                            ],
                            "recommendations": [
                                "Validate negative or zero stock quantities",
                                "Regularly update stock levels"
                            ]
                        }
                    ]
                },
                {
                    "name": "Warehouses",
                    "quality": {"good": 88, "bad": 10, "critical": 2},
                    "columns": [
                        {
                            "name": "warehouse_id",
                            "type": "integer",
                            "metrics": [
                                {"name": "Completeness", "value": 100, "status": "good"},
                                {"name": "Uniqueness", "value": 100, "status": "good"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "Sequential integers"},
                                {"name": "Min value", "value": 1},
                                {"name": "Max value", "value": 50}
                            ],
                            "recommendations": [
                                "No issues detected"
                            ]
                        },
                        {
                            "name": "location",
                            "type": "varchar(100)",
                            "metrics": [
                                {"name": "Completeness", "value": 90, "status": "good"},
                                {"name": "Consistency", "value": 85, "status": "bad"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "City, State format"},
                                {"name": "Invalid patterns", "value": "Free text (5%)"}
                            ],
                            "recommendations": [
                                "Standardize location format",
                                "Validate existing entries"
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "name": "Marketing",
            "tables": [
                {
                    "name": "Campaigns",
                    "quality": {"good": 75, "bad": 20, "critical": 5},
                    "columns": [
                        {
                            "name": "campaign_id",
                            "type": "integer",
                            "metrics": [
                                {"name": "Completeness", "value": 100, "status": "good"},
                                {"name": "Uniqueness", "value": 100, "status": "good"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "Sequential integers"},
                                {"name": "Min value", "value": 1},
                                {"name": "Max value", "value": 2345}
                            ],
                            "recommendations": [
                                "No issues detected"
                            ]
                        },
                        {
                            "name": "campaign_name",
                            "type": "varchar(200)",
                            "metrics": [
                                {"name": "Completeness", "value": 95, "status": "good"},
                                {"name": "Uniqueness", "value": 98, "status": "good"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "Alphanumeric campaign names"},
                                {"name": "Duplicate count", "value": "2 duplicates found"}
                            ],
                            "recommendations": [
                                "Ensure unique campaign names"
                            ]
                        },
                        {
                            "name": "start_date",
                            "type": "datetime",
                            "metrics": [
                                {"name": "Completeness", "value": 97, "status": "good"},
                                {"name": "Format", "value": 90, "status": "bad"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "YYYY-MM-DD"},
                                {"name": "Invalid patterns", "value": "Non-standard formats (3%)"}
                            ],
                            "recommendations": [
                                "Enforce consistent date format"
                            ]
                        }
                    ]
                },
                {
                    "name": "Leads",
                    "quality": {"good": 80, "bad": 15, "critical": 5},
                    "columns": [
                        {
                            "name": "lead_id",
                            "type": "integer",
                            "metrics": [
                                {"name": "Completeness", "value": 100, "status": "good"},
                                {"name": "Uniqueness", "value": 100, "status": "good"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "Sequential integers"},
                                {"name": "Min value", "value": 1},
                                {"name": "Max value", "value": 15000}
                            ],
                            "recommendations": [
                                "No issues detected"
                            ]
                        },
                        {
                            "name": "email",
                            "type": "varchar(100)",
                            "metrics": [
                                {"name": "Completeness", "value": 90, "status": "good"},
                                {"name": "Format", "value": 85, "status": "bad"},
                                {"name": "Uniqueness", "value": 92, "status": "bad"}
                            ],
                            "patterns": [
                                {"name": "Most common pattern", "value": "name@domain.com"},
                                {"name": "Invalid patterns", "value": "Non-email formats (10%)"}
                            ],
                            "recommendations": [
                                "Implement email validation",
                                "Remove duplicates"
                            ]
                        }
                    ]
                }
            ]
        }
    ]

    return dqData