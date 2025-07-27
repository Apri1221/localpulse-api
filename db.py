import duckdb
import threading
from contextlib import contextmanager
from typing import Optional, List, Tuple
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Thread-safe DuckDB connection manager"""
    def __init__(self, db_path: str = 'localpulse.db'):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._connection = None
        self._connection_count = 0
    
    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create a database connection"""
        if self._connection is None:
            try:
                self._connection = duckdb.connect(self.db_path)
                logger.info(f"Created new database connection to {self.db_path}")
            except Exception as e:
                logger.error(f"Failed to connect to database: {e}")
                raise
        return self._connection
    
    @contextmanager
    def get_connection(self):
        """Context manager for safe database connections"""
        with self._lock:
            self._connection_count += 1
            conn = self._get_connection()
            try:
                yield conn
            except Exception as e:
                logger.error(f"Database operation error: {e}")
                # Close connection on error to prevent corruption
                self.close_connection()
                raise
            finally:
                self._connection_count -= 1
                if self._connection_count == 0:
                    # Optional: close connection when not in use
                    # self.close_connection()
                    pass
    
    def close_connection(self):
        """Safely close the database connection"""
        with self._lock:
            if self._connection:
                try:
                    self._connection.close()
                    logger.info("Database connection closed")
                except Exception as e:
                    logger.error(f"Error closing connection: {e}")
                finally:
                    self._connection = None
                    self._connection_count = 0
    
    def execute_query(self, query: str, params: Optional[List] = None) -> List[Tuple]:
        """Execute a query safely"""
        with self.get_connection() as conn:
            try:
                if params:
                    result = conn.execute(query, params).fetchall()
                else:
                    result = conn.execute(query).fetchall()
                return result
            except Exception as e:
                logger.error(f"Query execution failed: {query[:100]}... Error: {e}")
                raise
    
    def execute_single(self, query: str, params: Optional[List] = None) -> Optional[Tuple]:
        """Execute a query and return single result"""
        with self.get_connection() as conn:
            try:
                if params:
                    result = conn.execute(query, params).fetchone()
                else:
                    result = conn.execute(query).fetchone()
                return result
            except Exception as e:
                logger.error(f"Single query execution failed: {query[:100]}... Error: {e}")
                raise
    
    def test_connection(self) -> bool:
        """Test database connection and basic operations"""
        try:
            with self.get_connection() as conn:
                # Test basic query
                version = conn.execute('SELECT version()').fetchone()
                logger.info(f"Database connection test successful. DuckDB version: {version[0]}")
                
                # Test table access
                tables = conn.execute('SHOW TABLES').fetchall()
                logger.info(f"Available tables: {[t[0] for t in tables]}")
                
                # Test poi_density table
                if 'poi_density' in [t[0] for t in tables]:
                    count = conn.execute('SELECT COUNT(*) FROM poi_density').fetchone()
                    logger.info(f"poi_density table has {count[0]} rows")
                
                return True
                
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

# Global database manager instance
db = DatabaseManager()

def get_poi_data(category: Optional[str] = None, district: Optional[str] = None, 
                limit: Optional[int] = None) -> List[Tuple]:
    """Safely retrieve POI data with optional filters"""
    
    query = '''
        SELECT category, district, latitude, longitude, intensity, 
               bank_category, bank_colorcode, name
        FROM poi_density
        WHERE 1=1
    '''
    params = []
    
    if category:
        query += ' AND category = ?'
        params.append(category)
    
    if district:
        query += ' AND district = ?'
        params.append(district)
    
    if limit:
        query += ' LIMIT ?'
        params.append(limit)
    
    return db.execute_query(query, params if params else None)

def get_categories() -> List[str]:
    """Get all unique categories"""
    result = db.execute_query('SELECT DISTINCT category FROM poi_density ORDER BY category')
    return [row[0] for row in result]

def get_districts() -> List[str]:
    """Get all unique districts"""
    result = db.execute_query('SELECT DISTINCT district FROM poi_density ORDER BY district')
    return [row[0] for row in result]

def get_bank_categories() -> List[str]:
    """Get all unique bank categories"""
    result = db.execute_query('''
        SELECT DISTINCT bank_category 
        FROM poi_density 
        WHERE bank_category IS NOT NULL 
        ORDER BY bank_category
    ''')
    return [row[0] for row in result]

def get_poi_summary() -> dict:
    """Get summary statistics about POI data"""
    with db.get_connection() as conn:
        total_pois = conn.execute('SELECT COUNT(*) FROM poi_density').fetchone()[0]
        
        categories = conn.execute('''
            SELECT category, COUNT(*) as count
            FROM poi_density 
            GROUP BY category 
            ORDER BY count DESC
        ''').fetchall()
        
        districts = conn.execute('''
            SELECT district, COUNT(*) as count
            FROM poi_density 
            GROUP BY district 
            ORDER BY count DESC
        ''').fetchall()
        
        bank_pois = conn.execute('''
            SELECT COUNT(*) 
            FROM poi_density 
            WHERE bank_category IS NOT NULL
        ''').fetchone()[0]
        
        return {
            'total_pois': total_pois,
            'categories': dict(categories),
            'districts': dict(districts),
            'bank_pois': bank_pois
        }

def main():
    """Test the database manager"""
    print("Testing Database Manager")
    print("=" * 30)
    
    # Test connection
    if not db.test_connection():
        print("❌ Database connection test failed")
        return
    
    print("✅ Database connection test passed")
    
    # Test data retrieval
    try:
        # Get summary
        summary = get_poi_summary()
        print(f"\n📊 Database Summary:")
        print(f"Total POIs: {summary['total_pois']}")
        print(f"Bank POIs: {summary['bank_pois']}")
        print(f"Categories: {len(summary['categories'])}")
        print(f"Districts: {len(summary['districts'])}")
        
        # Test category filter
        bank_data = get_poi_data(category='Bank', limit=5)
        print(f"\n🏦 Sample Bank data ({len(bank_data)} rows):")
        for row in bank_data[:3]:
            print(f"  {row[7]} - {row[0]} in {row[1]}")
        
        # Test district filter
        jakarta_data = get_poi_data(district='Jakarta Pusat', limit=5)
        print(f"\n🏙️ Jakarta Pusat data ({len(jakarta_data)} rows):")
        for row in jakarta_data[:3]:
            print(f"  {row[7]} - {row[0]}")
        
        print("\n✅ All database operations completed successfully")
        
    except Exception as e:
        print(f"❌ Database operation failed: {e}")
    
    finally:
        # Clean up
        db.close_connection()
        print("🔒 Database connection closed")

if __name__ == "__main__":
    main()