import os
import json
import duckdb
import anthropic
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

@dataclass
class MapDirective:
    """Instructions for map visualization based on LLM analysis"""
    mode: str  # "gdp", "whitespots", "risk", "heatmap", "financial", "business_analysis"
    filters: Dict[str, any]
    center: Optional[Tuple[float, float]] = None
    zoom: Optional[int] = None
    highlights: List[Dict] = None

class ClaudeService:
    def __init__(self, api_key: str, db_path: str = 'localpulse.db'):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.db_path = db_path
        self.conversation_history = []
        
        # Cache for database connections and common queries
        self._db_cache = {}
        
        # Simplified intent keywords for better performance
        self.INTENT_KEYWORDS = {
            'greeting': ['halo', 'hai', 'hello', 'apa kabar', 'selamat'],
            'whitespots': ['belum terjangkau', 'white-spot', 'white spot', 'lokasi terbaik', 'ekspansi'],
            'risk_assessment': ['cabang berisiko', 'pengawasan', 'berisiko'],
            'gdp_national': ['kondisi nasional', 'ekonomi nasional', 'gdp', 'nasional'],
            'business_analysis': ['coffee shop', 'kedai kopi', 'lokasi strategis', 'usaha'],
            'bank_distribution': ['sebaran bank', 'lokasi bank', 'cabang bank']
        }
        
        # Bank entity patterns
        self.BANK_ENTITIES = ['bni', 'bca', 'bri', 'mandiri', 'bsi', 'btn', 'cimb', 'danamon']
        
        # Default map centers
        self.MAP_CENTERS = {
            'Bali': (-8.6705, 115.2126),
            'Indonesia': (-2.5, 118)
        }
    
    def get_db_connection(self):
        """Get database connection with simple retry logic"""
        try:
            return duckdb.connect(self.db_path)
        except Exception as e:
            # Single retry
            try:
                return duckdb.connect(self.db_path)
            except Exception:
                raise e
    
    def extract_intent_and_entities(self, query: str) -> Tuple[str, List[str], str]:
        """Fast intent extraction using keyword matching (removed Claude API call for performance)"""
        query_lower = query.lower()
        
        # Intent classification using optimized keyword matching
        intent = 'general_info'  # default
        for intent_type, keywords in self.INTENT_KEYWORDS.items():
            if any(word in query_lower for word in keywords):
                intent = intent_type
                break
        
        # Entity extraction
        entities = []
        if 'bali' in query_lower:
            entities.append('Bali')
        
        # Extract bank entities
        bank_entities = [bank.upper() for bank in self.BANK_ENTITIES if bank in query_lower]
        entities.extend(bank_entities)
        
        # Location determination
        location = "Bali" if "bali" in query_lower else ("Indonesia" if intent == "gdp_national" else "Bali")
        
        return intent, entities, location
    
    def get_basic_stats(self, location: str) -> Dict:
        """Get basic database statistics (cached for performance)"""
        cache_key = f"basic_stats_{location}"
        
        if cache_key in self._db_cache:
            return self._db_cache[cache_key]
        
        try:
            conn = self.get_db_connection()
            
            stats = {
                'total_banks': conn.execute(
                    "SELECT COUNT(*) FROM poi_density WHERE category = 'Bank' AND province = ?", 
                    [location]
                ).fetchone()[0],
                'total_atms': conn.execute(
                    "SELECT COUNT(*) FROM poi_density WHERE category = 'ATM' AND province = ?", 
                    [location]
                ).fetchone()[0],
                'total_poi': conn.execute(
                    "SELECT COUNT(*) FROM poi_density WHERE category NOT IN ('Bank', 'ATM') AND province = ?", 
                    [location]
                ).fetchone()[0]
            }
            
            conn.close()
            self._db_cache[cache_key] = stats
            return stats
            
        except Exception as e:
            print(f"Database stats error: {e}")
            return {'total_banks': 0, 'total_atms': 0, 'total_poi': 0}
    
    def get_district_analysis(self, location: str) -> List:
        """Get district analysis for whitespots and risk assessment"""
        try:
            conn = self.get_db_connection()
            
            result = conn.execute("""
                SELECT d.district,
                       d.banks,
                       d.atms, 
                       d.total_financial,
                       COALESCE(p.avg_poi_density, 0) as avg_poi_density,
                       CASE 
                           WHEN p.avg_poi_density > 0.7 AND d.total_financial < 2 THEN 'HIGH_PRIORITY_WHITESPACE'
                           WHEN p.avg_poi_density > 0.5 AND d.total_financial < 3 THEN 'MEDIUM_PRIORITY_WHITESPACE'
                           WHEN p.avg_poi_density < 0.3 AND d.total_financial > 2 THEN 'POTENTIAL_RISK_OVERSUPPLY'
                           WHEN p.avg_poi_density < 0.2 AND d.total_financial > 0 THEN 'HIGH_RISK_LOW_DEMAND'
                           ELSE 'BALANCED'
                       END as area_classification
                FROM (
                    SELECT district,
                           COUNT(CASE WHEN category = 'Bank' THEN 1 END) as banks,
                           COUNT(CASE WHEN category = 'ATM' THEN 1 END) as atms,
                           COUNT(*) as total_financial
                    FROM poi_density 
                    WHERE category IN ('Bank', 'ATM') AND province = ?
                    GROUP BY district
                ) d
                LEFT JOIN (
                    SELECT district,
                           AVG(intensity) as avg_poi_density
                    FROM poi_density 
                    WHERE category NOT IN ('Bank', 'ATM') AND province = ?
                    GROUP BY district
                ) p ON d.district = p.district
                ORDER BY p.avg_poi_density DESC NULLS LAST
            """, [location, location]).fetchall()
            
            conn.close()
            return result
            
        except Exception as e:
            print(f"District analysis error: {e}")
            return []
    
    def get_business_opportunities(self, location: str) -> List:
        """Get business opportunity analysis with recommended areas"""
        try:
            conn = self.get_db_connection()
            
            opportunities = conn.execute("""
                SELECT district,
                       AVG(intensity) as avg_activity_density,
                       COUNT(*) as total_activity_points,
                       COUNT(CASE WHEN intensity > 0.7 THEN 1 END) as high_activity_spots,
                       ROUND(AVG(intensity) * COUNT(*), 2) as business_opportunity_score
                FROM poi_density 
                WHERE category NOT IN ('Bank', 'ATM') AND province = ?
                GROUP BY district
                HAVING AVG(intensity) > 0.5
                ORDER BY business_opportunity_score DESC
                LIMIT 10
            """, [location]).fetchall()
            
            conn.close()
            return opportunities
            
        except Exception as e:
            print(f"Business opportunities error: {e}")
            return []
    
    def get_database_context(self, intent: str, location: str = "Bali") -> Dict:
        """Get context based on intent - optimized for performance"""
        if not location or location == "None":
            location = "Bali"
        
        context = self.get_basic_stats(location)
        
        # Add intent-specific data only when needed
        if intent in ['whitespots', 'risk_assessment']:
            context['district_analysis'] = self.get_district_analysis(location)
        
        elif intent == 'business_analysis':
            context['business_opportunities'] = self.get_business_opportunities(location)
            # Add pre-defined recommended areas for Bali
            if location == "Bali":
                context['recommended_business_areas'] = [
                    {
                        'name': 'Seminyak Business District',
                        'coordinates': [-8.6872, 115.1748],
                        'district': 'Badung',
                        'business_potential': 'HIGH'
                    },
                    {
                        'name': 'Ubud Cultural Center', 
                        'coordinates': [-8.5088, 115.2623],
                        'district': 'Gianyar',
                        'business_potential': 'HIGH'
                    },
                    {
                        'name': 'Canggu Beach Area',
                        'coordinates': [-8.6482, 115.1374], 
                        'district': 'Badung',
                        'business_potential': 'HIGH'
                    }
                ]
        
        return context
    
    def generate_map_directive(self, intent: str, location: str) -> MapDirective:
        """Generate map visualization directives"""
        center = self.MAP_CENTERS.get(location, self.MAP_CENTERS['Bali'])
        zoom = 10 if location == "Bali" else 5
        
        directive_config = {
            "gdp_national": {
                "mode": "gdp",
                "filters": {},
                "center": self.MAP_CENTERS['Indonesia'],
                "zoom": 5
            },
            "whitespots": {
                "mode": "whitespots", 
                "filters": {"show_heatmap": True, "show_financial": True},
                "center": center,
                "zoom": zoom
            },
            "risk_assessment": {
                "mode": "risk",
                "filters": {"show_heatmap": True, "show_financial": True, "highlight_risk": True},
                "center": center,
                "zoom": zoom
            },
            "bank_distribution": {
                "mode": "financial",
                "filters": {"show_financial": True, "group_by_category": True},
                "center": center,
                "zoom": zoom
            },
            "business_analysis": {
                "mode": "business_analysis",
                "filters": {
                    "show_heatmap": True, 
                    "show_poi_density": True,
                    "show_financial": True,
                    "show_recommended_areas": True
                },
                "center": center,
                "zoom": 11
            }
        }
        
        # Get config or use default
        config = directive_config.get(intent, directive_config["gdp_national"])
        return MapDirective(**config)
    
    def create_system_prompt(self, intent: str, location: str, db_context: Dict) -> str:
        """Create appropriate system prompt based on complexity"""
        
        # Simple prompts for basic intents
        simple_intents = {
            'greeting': "You are LocalPulse.AI assistant. Say hello, mention you help with banking/economic analysis, ask how to help. Be friendly and concise (max 2 sentences).",
            'general_info': "You are LocalPulse.AI. Briefly explain you help analyze banking and economic data in Indonesia. Be concise (max 3 sentences).",
            'gdp_national': "You are LocalPulse.AI. Give a general economic overview for Indonesia without detailed analysis (max 4 sentences)."
        }
        
        if intent in simple_intents:
            return simple_intents[intent]
        
        # Comprehensive prompt for analytical intents
        return f"""
        You are LocalPulse.AI, an advanced economic and banking analysis system for Indonesia.

        **Analysis Context:**
        - Intent: {intent}
        - Location: {location}
        - Date: {datetime.now().strftime('%Y-%m-%d')}

        **Database Context:**
        {json.dumps(db_context, indent=2, default=str)}

        **Guidelines:**
        1. Use real data from the context
        2. Be specific with numbers and district names
        3. Provide actionable insights
        4. Match user's language (Indonesian/English)
        5. Structure clearly with headings and bullet points

        **Focus Areas:**
        - whitespots: High-opportunity areas with specific recommendations
        - risk_assessment: Branch vulnerability with risk classifications  
        - bank_distribution: Coverage analysis by district
        - business_analysis: Market opportunities with competitive analysis
        """
    
    def generate_response(self, query: str) -> Tuple[str, MapDirective]:
        """Generate intelligent response with map directives"""
        
        # Extract intent and entities
        intent, entities, location = self.extract_intent_and_entities(query)
        
        # Generate map directive
        map_directive = self.generate_map_directive(intent, location)
        
        # Determine analysis complexity
        needs_deep_analysis = intent in ['whitespots', 'risk_assessment', 'business_analysis', 'bank_distribution']
        
        # Get appropriate context
        if needs_deep_analysis:
            db_context = self.get_database_context(intent, location)
            max_tokens = 800
        else:
            db_context = self.get_basic_stats(location)
            max_tokens = 200
        
        # Create system prompt
        system_prompt = self.create_system_prompt(intent, location, db_context)
        
        # Generate Claude response
        try:
            response = self.client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=max_tokens,
                system=system_prompt,
                messages=[{"role": "user", "content": f'User Query: "{query}"\n\nProvide helpful response based on context.'}]
            )
            
            # Store in conversation history
            self.conversation_history.append({
                "timestamp": datetime.now().isoformat(),
                "query": query,
                "response": response.content[0].text
            })
            
            return response.content[0].text, map_directive
            
        except Exception as e:
            print(f"Claude API error: {e}")
            
            # Fallback response with available data
            error_response = f"""
            **⚠️ Analisis Terbatas - API Error**
            
            Data {location}:
            - Bank: {db_context.get('total_banks', 0)} lokasi
            - ATM: {db_context.get('total_atms', 0)} lokasi  
            - POI: {db_context.get('total_poi', 0)} titik aktivitas
            
            Error: {str(e)}
            """
            return error_response, map_directive
    
    def get_conversation_history(self) -> List[Dict]:
        """Get conversation history"""
        return self.conversation_history
    
    def clear_conversation_history(self):
        """Clear conversation history and cache"""
        self.conversation_history = []
        self._db_cache = {}

# Usage example
if __name__ == "__main__":
    api_key = os.getenv('CLAUDE_API_KEY')
    if not api_key:
        print("Please set CLAUDE_API_KEY environment variable")
        exit(1)
    
    service = ClaudeService(api_key)
    
    # Test queries
    test_queries = [
        "Halo, apa kabar?",
        "Lokasi belum terjangkau bank di Bali?",
        "Cabang berisiko di Bali?",
        "Lokasi strategis untuk coffee shop di Bali?"
    ]
    
    for query in test_queries:
        print(f"\n{'='*50}")
        print(f"Query: {query}")
        print('='*50)
        
        response, directive = service.generate_response(query)
        print(f"Response: {response}")
        print(f"Map Directive: {directive.mode}")