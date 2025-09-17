import requests
import json
import os
from flask import current_app
import logging

logger = logging.getLogger(__name__)

class AIHarmonizerService:
    def __init__(self):
        self.ollama_host = current_app.config.get('OLLAMA_HOST', 'http://localhost:11434')
        self.ollama_model = current_app.config.get('OLLAMA_MODEL', 'llama3')
        self.cohere_api_key = current_app.config.get('COHERE_API_KEY', os.getenv('COHERE_API_KEY'))
        self.cohere_model = current_app.config.get('COHERE_MODEL', 'command')
        self.preferred_provider = current_app.config.get('AI_PREFERRED_PROVIDER', 'ollama')
    
    def generate_column_description(self, table_name, column_info, sample_values):
        """Generate column description using AI (Ollama first, Cohere as fallback)"""
        prompt = self._build_description_prompt(table_name, column_info, sample_values)
        
        # Try Ollama first if preferred
        if self.preferred_provider == 'ollama':
            try:
                description = self._generate_with_ollama(prompt)
                if description:
                    return description
            except Exception as e:
                logger.warning(f"Ollama failed, falling back to Cohere: {e}")
        
        # Try Cohere
        try:
            description = self._generate_with_cohere(prompt)
            if description:
                return description
        except Exception as e:
            logger.error(f"Cohere also failed: {e}")
        
        # If both fail, use fallback
        return self._fallback_description(table_name, column_info, sample_values)
    
    def suggest_mappings(self, source_table, target_tables, all_columns):
        """Suggest column mappings using AI"""
        prompt_data = {
            "source_table": source_table,
            "target_tables": target_tables,
            "columns": all_columns
        }
        
        # Try Ollama first if preferred
        if self.preferred_provider == 'ollama':
            try:
                suggestions = self._suggest_with_ollama(prompt_data)
                if suggestions:
                    return suggestions
            except Exception as e:
                logger.warning(f"Ollama mapping suggestion failed, falling back to Cohere: {e}")
        
        # Try Cohere
        try:
            suggestions = self._suggest_with_cohere(prompt_data)
            if suggestions:
                return suggestions
        except Exception as e:
            logger.error(f"Cohere mapping suggestion also failed: {e}")
        
        # If both fail, use fallback
        return self._fallback_suggestions(source_table, target_tables, all_columns)
    
    def _generate_with_ollama(self, prompt):
        """Generate description using Ollama"""
        try:
            response = requests.post(
                f"{self.ollama_host}/api/generate",
                json={
                    "model": self.ollama_model,
                    "prompt": prompt,
                    "options": {"temperature": 0.2},
                    "stream": False
                },
                timeout=120
            )
            response.raise_for_status()
            return response.json()['response'].strip()
        except Exception as e:
            logger.error(f"Ollama generation failed: {e}")
            raise
    
    def _generate_with_cohere(self, prompt):
        """Generate description using Cohere"""
        if not self.cohere_api_key:
            logger.error("Cohere API key not configured")
            raise ValueError("Cohere API key not available")
        
        try:
            import cohere
            co = cohere.Client(self.cohere_api_key)
            response = co.generate(
                model=self.cohere_model,
                prompt=prompt,
                max_tokens=200,
                temperature=0.2
            )
            return response.generations[0].text.strip()
        except ImportError:
            logger.error("Cohere Python package not installed. Install with: pip install cohere")
            raise
        except Exception as e:
            logger.error(f"Cohere generation failed: {e}")
            raise
    
    def _suggest_with_ollama(self, prompt_data):
        """Suggest mappings using Ollama"""
        prompt = self._build_mapping_prompt(prompt_data)
        
        try:
            response = requests.post(
                f"{self.ollama_host}/api/generate",
                json={
                    "model": self.ollama_model,
                    "prompt": prompt,
                    "options": {"temperature": 0.1},
                    "stream": False
                },
                timeout=180
            )
            response.raise_for_status()
            
            result = response.json()['response'].strip()
            return self._parse_ai_response(result)
                
        except Exception as e:
            logger.error(f"Ollama mapping suggestion failed: {e}")
            raise
    
    def _suggest_with_cohere(self, prompt_data):
        """Suggest mappings using Cohere"""
        if not self.cohere_api_key:
            logger.error("Cohere API key not configured")
            raise ValueError("Cohere API key not available")
        
        prompt = self._build_mapping_prompt(prompt_data)
        
        try:
            import cohere
            co = cohere.Client(self.cohere_api_key)
            response = co.generate(
                model=self.cohere_model,
                prompt=prompt,
                max_tokens=500,
                temperature=0.1
            )
            
            result = response.generations[0].text.strip()
            return self._parse_ai_response(result)
            
        except ImportError:
            logger.error("Cohere Python package not installed. Install with: pip install cohere")
            raise
        except Exception as e:
            logger.error(f"Cohere mapping suggestion failed: {e}")
            raise
    
    def _build_description_prompt(self, table_name, column_info, sample_values):
        """Build prompt for column description"""
        column_name = self._get_column_name_from_metadata(column_info)
        
        return f"""
        As a data governance expert, provide a concise description for:
        
        Table: {table_name}
        Column: {column_name}
        Type: {column_info.get('DATA_TYPE', column_info.get('data_type', 'N/A'))}
        Nullable: {column_info.get('IS_NULLABLE', column_info.get('is_nullable', 'N/A'))}
        Max Length: {column_info.get('CHARACTER_MAXIMUM_LENGTH', column_info.get('character_maximum_length', 'N/A'))}
        
        Sample Values: {sample_values[:5]}
        
        Include:
        - **Business Purpose**: Describe the column's role in business processes (1 sentence).
        - **Data Quality Rules**: List 1-2 rules to ensure data integrity.
        - **Example Usage**: Provide 1 example of how the column is used.
        - **Known Issues/Limitations**: Identify 1-2 possible data quality issues.
        
        Format as markdown bullets, keep total length under 200 words.
        """
    
    def _build_mapping_prompt(self, prompt_data):
        """Build prompt for mapping suggestions"""
        return f"""
        Analyze these database tables and suggest column mappings for data integration:
        
        Source Table: {prompt_data['source_table']}
        Target Tables: {', '.join(prompt_data['target_tables'])}
        
        Available Columns:
        {json.dumps(prompt_data['columns'], indent=2)}
        
        Suggest mappings between source and target columns based on:
        1. Column name similarity
        2. Data type compatibility
        3. Business purpose alignment
        4. Semantic meaning
        
        For each suggestion, provide:
        - Source table and column
        - Target table and column
        - Confidence score (0-1)
        - Reason for the mapping
        
        Return JSON format:
        {{
            "suggestions": [
                {{
                    "source_table": "table_name",
                    "source_column": "column_name",
                    "target_table": "table_name",
                    "target_column": "column_name",
                    "confidence": 0.95,
                    "reason": "Explanation of why these columns should be mapped"
                }}
            ]
        }}
        """
    
    def _parse_ai_response(self, response_text):
        """Parse AI response to extract JSON"""
        try:
            # Extract JSON from the response
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1
            if json_start != -1 and json_end != -1:
                json_str = response_text[json_start:json_end]
                result = json.loads(json_str)
                return result.get('suggestions', [])
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse AI response as JSON: {e}")
        
        return []
    
    def _get_column_name_from_metadata(self, col_meta):
        """Extract column name from metadata"""
        possible_keys = ['COLUMN_NAME', 'column_name', 'Column_Name', 'COLUMN', 'column', 'name', 'NAME']
        for key in possible_keys:
            if key in col_meta:
                return col_meta[key]
        return 'Unknown'
    
    def _fallback_description(self, table_name, column_info, sample_values):
        """Fallback description when AI fails"""
        column_name = self._get_column_name_from_metadata(column_info)
        
        return f"""
- **Business Purpose**: Stores {column_name} information for {table_name}
- **Data Quality Rules**: Ensure data matches type {column_info.get('DATA_TYPE', column_info.get('data_type', 'unknown'))}
- **Example Usage**: Used in data analysis and reporting
- **Known Issues/Limitations**: Sample values: {sample_values[:3]}
        """
    
    def _fallback_suggestions(self, source_table, target_tables, all_columns):
        """Fallback suggestions when AI fails"""
        suggestions = []
        source_columns = all_columns.get(source_table, [])
        
        for target_table in target_tables:
            target_columns = all_columns.get(target_table, [])
            
            for src_col in source_columns:
                for tgt_col in target_columns:
                    src_name = src_col['column_name'].lower()
                    tgt_name = tgt_col['column_name'].lower()
                    
                    # Exact match
                    if src_name == tgt_name:
                        suggestions.append({
                            "source_table": source_table,
                            "source_column": src_col['column_name'],
                            "target_table": target_table,
                            "target_column": tgt_col['column_name'],
                            "confidence": 0.95,
                            "reason": "Exact column name match"
                        })
                    
                    # Common patterns
                    patterns = [
                        ('id', 'id'), ('name', 'name'), ('date', 'date'),
                        ('email', 'email'), ('amount', 'price'), ('total', 'sum'),
                        ('first', 'given'), ('last', 'family'), ('created', 'date_created')
                    ]
                    
                    for pattern1, pattern2 in patterns:
                        if pattern1 in src_name and pattern2 in tgt_name:
                            suggestions.append({
                                "source_table": source_table,
                                "source_column": src_col['column_name'],
                                "target_table": target_table,
                                "target_column": tgt_col['column_name'],
                                "confidence": 0.85,
                                "reason": f"Pattern match: {pattern1} -> {pattern2}"
                            })
        
        return suggestions

# Global AI service instance
ai_harmonizer_service = AIHarmonizerService()