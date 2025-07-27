import os
import duckdb
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from rag import ClaudeService
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Initialize Claude service
CLAUDE_API_KEY = os.getenv('CLAUDE_API_KEY')
if not CLAUDE_API_KEY:
    print("⚠️ CLAUDE_API_KEY not set. Claude features will be disabled.")
    claude_service = None
else:
    claude_service = ClaudeService(CLAUDE_API_KEY)

# Database connection
DB_PATH = 'localpulse.db'

def get_db_connection():
    """Get database connection"""
    return duckdb.connect(DB_PATH)

@app.route('/<path:filename>')
def static_files(filename):
    return send_from_directory('.', filename)

@app.route('/api/health', methods=['GET'])
def health_check():
    """Enhanced health check with Claude service status"""
    try:
        conn = get_db_connection()
        poi_total = conn.execute("SELECT COUNT(*) FROM poi_density").fetchone()[0]
        poi_financial = conn.execute("SELECT COUNT(*) FROM poi_density WHERE category IN ('Bank', 'ATM')").fetchone()[0]
        poi_density = conn.execute("SELECT COUNT(*) FROM poi_density WHERE category NOT IN ('Bank', 'ATM')").fetchone()[0]
        conn.close()
        
        return jsonify({
            'success': True,
            'status': 'healthy',
            'database': {
                'poi_total': poi_total,
                'poi_financial': poi_financial,
                'poi_density': poi_density
            },
            'claude_service': {
                'enabled': claude_service is not None,
                'status': 'ready' if claude_service else 'disabled'
            },
            'endpoints': [
                '/api/financial',
                '/api/poi', 
                '/api/health',
                '/api/chat',
                '/api/search'
            ]
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/chat', methods=['POST'])
def chat_endpoint():
    """Main chat endpoint with Claude LLM integration"""
    try:
        data = request.get_json()
        if not data or 'query' not in data:
            return jsonify({
                'success': False,
                'error': 'Query is required'
            }), 400
        
        query = data['query'].strip()
        
        if not claude_service:
            return jsonify({
                'success': False,
                'error': 'Claude service not available. Please set CLAUDE_API_KEY environment variable.'
            }), 503
        
        # Generate response with Claude
        print(f"Processing query: {query}")
        response_text, map_directive = claude_service.generate_response(query)
        print(f"Response generated: {len(response_text)} characters")
        
        return jsonify({
            'success': True,
            'query': query,
            'response': response_text,
            'map_directive': {
                'mode': map_directive.mode,
                'filters': map_directive.filters,
                'center': map_directive.center,
                'zoom': map_directive.zoom,
                'highlights': map_directive.highlights or []
            },
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"Chat endpoint error: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/search', methods=['POST'])
def search_endpoint():
    """Web search integration for real-time data"""
    try:
        data = request.get_json()
        query = data.get('query', '')
        location = data.get('location', 'Bali')
        
        # Implement web search logic
        search_results = perform_web_search(query, location)
        
        return jsonify({
            'success': True,
            'query': query,
            'location': location,
            'results': search_results,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def perform_web_search(query: str, location: str) -> list:
    """Perform web search for real-time data"""
    # Placeholder implementation
    # In production, integrate with:
    # - Google Search API
    # - Bing Search API  
    # - SerpAPI
    # - Custom web scraping
    
    search_keywords = f"{query} {location} bank ATM ekonomi"
    
    # Return empty results for now - web search integration to be implemented
    search_results = []
    
    return search_results

@app.route('/api/conversation', methods=['GET'])
def get_conversation_history():
    """Get conversation history"""
    if not claude_service:
        return jsonify({'success': False, 'error': 'Claude service not available'}), 503
    
    try:
        history = claude_service.get_conversation_history()
        return jsonify({
            'success': True,
            'history': history
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/conversation', methods=['DELETE'])
def clear_conversation_history():
    """Clear conversation history"""
    if not claude_service:
        return jsonify({'success': False, 'error': 'Claude service not available'}), 503
    
    try:
        claude_service.clear_conversation_history()
        return jsonify({
            'success': True,
            'message': 'Conversation history cleared'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/financial', methods=['GET'])
def get_financial_data():
    """Get financial institutions data from poi_density table"""
    try:
        conn = get_db_connection()
        
        # Get query parameters
        type_filter = request.args.get('type')
        province_filter = request.args.get('province', 'Bali')
        district_filter = request.args.get('district')
        
        # Build query for Bank and ATM categories only
        query = """
            SELECT id, name, category, latitude, longitude, province, district, 
                   rating, rating_count, gmaps_link, bank_category, bank_colorcode
            FROM poi_density 
            WHERE category IN ('Bank', 'ATM') AND province = ?
        """
        params = [province_filter]
        
        if district_filter:
            query += " AND district = ?"
            params.append(district_filter)
            
        if type_filter and type_filter.upper() in ['BANK', 'ATM']:
            query += " AND category = ?"
            params.append(type_filter.title())
        
        query += " ORDER BY category, name"
        
        # Execute query
        result = conn.execute(query, params).fetchall()
        conn.close()
        
        # Format response
        data = {
            'success': True,
            'count': len(result),
            'data': {
                'banks': [],
                'atms': []
            }
        }
        
        for row in result:
            institution = {
                'id': row[0],
                'name': row[1],
                'type': row[2],  # category
                'lat': row[3],
                'lng': row[4],
                'province': row[5],
                'district': row[6],
                'rating': row[7],
                'rating_count': row[8],
                'gmaps_link': row[9],
                'bank_category': row[10],
                'bank_colorcode': row[11]
            }
            
            if row[2] == 'Bank':
                data['data']['banks'].append(institution)
            else:
                data['data']['atms'].append(institution)
        
        return jsonify(data)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/poi', methods=['GET'])
def get_poi_data():
    """Get POI density data (excluding Bank and ATM categories)"""
    try:
        conn = get_db_connection()
        
        # Get query parameters
        province_filter = request.args.get('province', 'Bali')
        district_filter = request.args.get('district')
        category_filter = request.args.get('category')
        min_intensity = float(request.args.get('min_intensity', 0))
        max_intensity = float(request.args.get('max_intensity', 1))
        
        # Build query - exclude Bank and ATM for density analysis
        query = """
            SELECT id, name, latitude, longitude, intensity, category, province, district,
                   rating, rating_count, gmaps_link
            FROM poi_density 
            WHERE category NOT IN ('Bank', 'ATM') 
            AND province = ? AND intensity >= ? AND intensity <= ?
        """
        params = [province_filter, min_intensity, max_intensity]
        
        if district_filter:
            query += " AND district = ?"
            params.append(district_filter)
            
        if category_filter:
            query += " AND category = ?"
            params.append(category_filter)
        
        query += " ORDER BY intensity DESC, category"
        
        # Execute query
        result = conn.execute(query, params).fetchall()
        
        # Format response for heatmap (simplified format)
        heatmap_data = []
        detailed_data = []
        
        for row in result:
            # For heatmap: [lat, lng, intensity]
            heatmap_data.append([row[2], row[3], row[4]])
            
            # For detailed view
            detailed_data.append({
                'id': row[0],
                'name': row[1],
                'lat': row[2],
                'lng': row[3],
                'intensity': row[4],
                'category': row[5],
                'province': row[6],
                'district': row[7],
                'rating': row[8],
                'rating_count': row[9],
                'gmaps_link': row[10]
            })
        
        # Get district/category summary
        district_result = conn.execute("""
            SELECT district, category, COUNT(*) as count, AVG(intensity) as avg_intensity
            FROM poi_density 
            WHERE category NOT IN ('Bank', 'ATM') AND province = ?
            GROUP BY district, category
            ORDER BY avg_intensity DESC
        """, [province_filter]).fetchall()
        
        districts = []
        for row in district_result:
            districts.append({
                'district': row[0],
                'category': row[1],
                'count': row[2],
                'avg_intensity': round(row[3], 3)
            })
        
        # Get overall district summary
        district_summary = conn.execute("""
            SELECT district, COUNT(*) as count, AVG(intensity) as avg_intensity,
                   COUNT(DISTINCT category) as categories
            FROM poi_density 
            WHERE category NOT IN ('Bank', 'ATM') AND province = ?
            GROUP BY district
            ORDER BY avg_intensity DESC
        """, [province_filter]).fetchall()
        
        summary = []
        for row in district_summary:
            summary.append({
                'district': row[0],
                'count': row[1],
                'avg_intensity': round(row[2], 3),
                'categories': row[3]
            })
        
        conn.close()
        
        # Response format
        response_data = {
            'success': True,
            'count': len(result),
            'heatmap_data': heatmap_data,
            'detailed_data': detailed_data,
            'districts': districts,
            'district_summary': summary
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    # Check if database exists
    if not os.path.exists(DB_PATH):
        print("❌ Database not found. Please run setup_database.py first.")
        exit(1)
    
    print("🚀 Enhanced LocalPulse API Server with Claude LLM starting...")
    print(f"📡 Listening on http://0.0.0.0:8081")
    print(f"🤖 Claude LLM: {'Enabled' if claude_service else 'Disabled (set CLAUDE_API_KEY)'}")
    print(f"🔗 Available endpoints:")
    print(f"   - http://0.0.0.0:8081/api/health")
    print(f"   - http://0.0.0.0:8081/api/chat (POST)")
    print(f"   - http://0.0.0.0:8081/api/financial")
    print(f"   - http://0.0.0.0:8081/api/poi")
    print(f"   - http://0.0.0.0:8081/api/search (POST)")
    print("=" * 50)
    
    app.run(host='0.0.0.0', port=8081, debug=False)