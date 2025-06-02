import os
import json
import logging
import traceback
from datetime import datetime
from functools import wraps
from typing import Optional, Dict, Any

from flask import Flask, request, jsonify
from pymongo import MongoClient, errors as mongo_errors
from pymongo.collection import Collection
from bson import ObjectId
import urllib.parse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize Flask app
app = Flask(__name__)

# Configuration from environment variables only
MONGO_URI = os.getenv('MONGO_URI')
DATABASE_NAME = os.getenv('DATABASE_NAME')
WEBHOOK_USERNAME = os.getenv('WEBHOOK_USERNAME')
WEBHOOK_PASSWORD = os.getenv('WEBHOOK_PASSWORD')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
PORT = int(os.getenv('PORT', 5000))

# Validate required environment variables
required_vars = ['MONGO_URI', 'DATABASE_NAME', 'WEBHOOK_USERNAME', 'WEBHOOK_PASSWORD']
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
    print("📝 Please create a .env file with:")
    for var in missing_vars:
        print(f"   {var}=your_value_here")
    exit(1)

# Configure comprehensive logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Console output for Koyeb
    ]
)
logger = logging.getLogger(__name__)

# Disable werkzeug logs for cleaner output
logging.getLogger('werkzeug').setLevel(logging.WARNING)

class DatabaseManager:
    """Manages MongoDB Atlas connection with proper error handling"""
    
    def __init__(self):
        self.client = None
        self.db = None
        self.collections = {}
        self._connect()
    
    def _connect(self):
        """Establish MongoDB connection with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"🔌 Attempting MongoDB connection (attempt {attempt + 1}/{max_retries})")
                
                # Parse URI to hide password in logs
                parsed_uri = urllib.parse.urlparse(MONGO_URI)
                safe_uri = f"{parsed_uri.scheme}://{parsed_uri.hostname}:{parsed_uri.port}/{parsed_uri.path}"
                logger.info(f"🌐 Connecting to: {safe_uri}")
                
                self.client = MongoClient(
                    MONGO_URI,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000,
                    socketTimeoutMS=5000,
                    maxPoolSize=10,
                    retryWrites=True
                )
                
                # Test connection
                self.client.admin.command('ping')
                self.db = self.client[DATABASE_NAME]
                
                # Initialize collections
                self.collections = {
                    'events': self.db.events,
                    'patients': self.db.patients,
                    'locations': self.db.locations,
                    'professionals': self.db.professionals,
                    'professions': self.db.professions,
                    'appointment_types': self.db.appointment_types
                }
                
                logger.info(f"✅ Connected to MongoDB Atlas successfully!")
                logger.info(f"📊 Database: {DATABASE_NAME}")
                logger.info(f"🏥 Collections: {list(self.collections.keys())}")
                return
                
            except Exception as e:
                logger.error(f"❌ MongoDB connection attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    logger.critical("💥 Failed to connect to MongoDB after all retries")
                    raise ConnectionError(f"Cannot connect to MongoDB: {str(e)}")
    
    def get_collection(self, name: str) -> Collection:
        """Get collection with error handling"""
        if name not in self.collections:
            raise ValueError(f"Collection '{name}' not found")
        return self.collections[name]
    
    def health_check(self) -> Dict[str, Any]:
        """Check database health"""
        try:
            self.client.admin.command('ping')
            stats = {
                'status': 'healthy',
                'database': DATABASE_NAME,
                'collections': {}
            }
            
            for name, collection in self.collections.items():
                try:
                    count = collection.count_documents({})
                    stats['collections'][name] = count
                except Exception as e:
                    stats['collections'][name] = f"error: {str(e)}"
            
            return stats
        except Exception as e:
            logger.error(f"💔 Database health check failed: {e}")
            return {'status': 'unhealthy', 'error': str(e)}

# Initialize database manager
try:
    db_manager = DatabaseManager()
except Exception as e:
    logger.critical(f"💥 Failed to initialize database: {e}")
    exit(1)

def require_auth(f):
    """Authentication decorator with proper error handling"""
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            auth = request.authorization
            if not auth:
                logger.warning(f"🚫 No auth provided from {request.remote_addr}")
                return jsonify({
                    'error': 'Authentication required',
                    'message': 'HTTP Basic Auth credentials required'
                }), 401
            
            if not (auth.username == WEBHOOK_USERNAME and auth.password == WEBHOOK_PASSWORD):
                logger.warning(f"🚫 Invalid credentials from {request.remote_addr} - username: {auth.username}")
                return jsonify({
                    'error': 'Invalid credentials',
                    'message': 'Invalid username or password'
                }), 401
            
            logger.debug(f"✅ Auth successful for {auth.username} from {request.remote_addr}")
            return f(*args, **kwargs)
            
        except Exception as e:
            logger.error(f"💥 Auth error: {e}")
            return jsonify({
                'error': 'Authentication error',
                'message': 'Error during authentication'
            }), 500
    
    return decorated

def parse_datetime(date_string: str) -> Optional[datetime]:
    """Parse various datetime formats with comprehensive error handling"""
    if not date_string:
        return None
    
    try:
        # Handle ISO format with timezone
        if 'T' in date_string:
            if date_string.endswith('Z'):
                return datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            return datetime.fromisoformat(date_string)
        # Handle date only format
        return datetime.strptime(date_string, '%Y-%m-%d')
    except Exception as e:
        logger.error(f"❌ Error parsing datetime '{date_string}': {e}")
        return None

def safe_upsert(collection: Collection, filter_doc: Dict, update_doc: Dict, entity_type: str, entity_id: str) -> Optional[ObjectId]:
    """Safely upsert document with comprehensive error handling"""
    try:
        result = collection.update_one(
            filter_doc,
            {
                '$set': update_doc,
                '$setOnInsert': {'created_at': datetime.utcnow()}
            },
            upsert=True
        )
        
        action = 'created' if result.upserted_id else 'updated'
        logger.info(f"✅ {entity_type} ID:{entity_id} {action}")
        
        return result.upserted_id or collection.find_one(filter_doc)['_id']
        
    except mongo_errors.DuplicateKeyError as e:
        logger.warning(f"⚠️ Duplicate key for {entity_type} ID:{entity_id}: {e}")
        return collection.find_one(filter_doc)['_id']
    except Exception as e:
        logger.error(f"💥 Error upserting {entity_type} ID:{entity_id}: {e}")
        raise

def process_patient(patient_data: Dict) -> Optional[ObjectId]:
    """Process patient data with comprehensive field mapping"""
    if not patient_data or 'id' not in patient_data:
        logger.warning("⚠️ No patient data or missing ID")
        return None
    
    try:
        address_data = patient_data.get('address', {})
        
        patient_doc = {
            'onedoc_id': patient_data['id'],
            'first_name': patient_data.get('firstName'),
            'last_name': patient_data.get('lastName'),
            'gender': patient_data.get('gender'),
            'birth_date': parse_datetime(patient_data.get('birthDate')),
            'email': patient_data.get('email'),
            'mobile_phone_number': patient_data.get('mobilePhoneNumber'),
            'language': patient_data.get('lang'),
            'primary_insurance_number': patient_data.get('primaryInsuranceNumber'),
            'address_street': address_data.get('street'),
            'address_street_number': address_data.get('streetNumber'),
            'address_zip_code': address_data.get('zipCode'),
            'address_city': address_data.get('city'),
            'address_state': address_data.get('state'),
            'address_country': address_data.get('country'),
            'address_full': address_data,
            'updated_at': datetime.utcnow(),
            'source': 'onedoc_webhook'
        }
        
        # Remove None values
        patient_doc = {k: v for k, v in patient_doc.items() if v is not None}
        
        patient_name = f"{patient_data.get('firstName', '')} {patient_data.get('lastName', '')}".strip()
        logger.debug(f"👤 Processing patient: {patient_name} (ID: {patient_data['id']})")
        
        return safe_upsert(
            db_manager.get_collection('patients'),
            {'onedoc_id': patient_data['id']},
            patient_doc,
            'Patient',
            str(patient_data['id'])
        )
        
    except Exception as e:
        logger.error(f"💥 Error processing patient ID:{patient_data.get('id')}: {e}")
        logger.error(f"Patient data: {json.dumps(patient_data, indent=2)}")
        raise

def process_location(location_data: Dict) -> Optional[ObjectId]:
    """Process location data"""
    if not location_data or 'id' not in location_data:
        return None
    
    try:
        location_doc = {
            'onedoc_id': location_data['id'],
            'name': location_data.get('name'),
            'updated_at': datetime.utcnow(),
            'source': 'onedoc_webhook'
        }
        
        location_doc = {k: v for k, v in location_doc.items() if v is not None}
        
        return safe_upsert(
            db_manager.get_collection('locations'),
            {'onedoc_id': location_data['id']},
            location_doc,
            'Location',
            str(location_data['id'])
        )
        
    except Exception as e:
        logger.error(f"💥 Error processing location ID:{location_data.get('id')}: {e}")
        raise

def process_professional(professional_data: Dict) -> Optional[ObjectId]:
    """Process professional data"""
    if not professional_data or 'id' not in professional_data:
        return None
    
    try:
        professional_doc = {
            'onedoc_id': professional_data['id'],
            'name': professional_data.get('name'),
            'updated_at': datetime.utcnow(),
            'source': 'onedoc_webhook'
        }
        
        professional_doc = {k: v for k, v in professional_doc.items() if v is not None}
        
        return safe_upsert(
            db_manager.get_collection('professionals'),
            {'onedoc_id': professional_data['id']},
            professional_doc,
            'Professional',
            str(professional_data['id'])
        )
        
    except Exception as e:
        logger.error(f"💥 Error processing professional ID:{professional_data.get('id')}: {e}")
        raise

def process_profession(profession_data: Dict) -> Optional[ObjectId]:
    """Process profession data"""
    if not profession_data or 'id' not in profession_data:
        return None
    
    try:
        profession_doc = {
            'onedoc_id': profession_data['id'],
            'name': profession_data.get('name'),
            'updated_at': datetime.utcnow(),
            'source': 'onedoc_webhook'
        }
        
        profession_doc = {k: v for k, v in profession_doc.items() if v is not None}
        
        return safe_upsert(
            db_manager.get_collection('professions'),
            {'onedoc_id': profession_data['id']},
            profession_doc,
            'Profession',
            str(profession_data['id'])
        )
        
    except Exception as e:
        logger.error(f"💥 Error processing profession ID:{profession_data.get('id')}: {e}")
        raise

def process_appointment_type(appointment_type_data: Dict) -> Optional[ObjectId]:
    """Process appointment type data"""
    if not appointment_type_data or 'id' not in appointment_type_data:
        return None
    
    try:
        appointment_type_doc = {
            'onedoc_id': appointment_type_data['id'],
            'profession_id': appointment_type_data.get('professionId'),
            'name': appointment_type_data.get('name'),
            'duration_minutes': appointment_type_data.get('durationMinutes'),
            'updated_at': datetime.utcnow(),
            'source': 'onedoc_webhook'
        }
        
        appointment_type_doc = {k: v for k, v in appointment_type_doc.items() if v is not None}
        
        return safe_upsert(
            db_manager.get_collection('appointment_types'),
            {'onedoc_id': appointment_type_data['id']},
            appointment_type_doc,
            'AppointmentType',
            str(appointment_type_data['id'])
        )
        
    except Exception as e:
        logger.error(f"💥 Error processing appointment type ID:{appointment_type_data.get('id')}: {e}")
        raise

@app.route('/events', methods=['POST'])
@require_auth
def handle_onedoc_event():
    """Main OneDoc webhook handler with comprehensive error handling"""
    request_start = datetime.utcnow()
    event_id = 'unknown'
    
    try:
        # Validate request
        if not request.is_json:
            logger.error("❌ Request is not JSON")
            return jsonify({
                'error': 'Invalid request',
                'message': 'Content-Type must be application/json'
            }), 400
        
        event_data = request.get_json()
        if not event_data:
            logger.error("❌ Empty JSON payload")
            return jsonify({
                'error': 'Empty payload',
                'message': 'JSON payload is required'
            }), 400
        
        # Validate required fields
        if 'id' not in event_data:
            logger.error("❌ Missing required field: id")
            return jsonify({
                'error': 'Missing required field',
                'message': 'Event ID is required'
            }), 400
        
        # Validate event ID is numeric
        try:
            event_id = int(event_data['id'])
        except (ValueError, TypeError):
            logger.error(f"❌ Invalid event ID: {event_data.get('id')}")
            return jsonify({
                'error': 'Invalid event ID',
                'message': 'Event ID must be a number'
            }), 400
        
        event_id = event_data['id']  # Now we know it exists and is valid
        patient_info = event_data.get('patient', {})
        patient_name = f"{patient_info.get('firstName', '')} {patient_info.get('lastName', '')}".strip()
        
        logger.info(f"📥 WEBHOOK RECEIVED - Event ID: {event_id} | Patient: {patient_name}")
        logger.debug(f"📋 Full payload: {json.dumps(event_data, indent=2)}")
        
        # Process all related entities
        logger.info("🔄 Processing related entities...")
        
        patient_ref = process_patient(event_data.get('patient'))
        location_ref = process_location(event_data.get('location'))
        professional_ref = process_professional(event_data.get('professional'))
        profession_ref = process_profession(event_data.get('profession'))
        appointment_type_ref = process_appointment_type(event_data.get('appointmentType'))
        
        # Create main event document
        logger.info(f"📅 Creating main event document for ID: {event_id}")
        
        event_doc = {
            'onedoc_id': event_data.get('id'),
            'calendar_id': event_data.get('calendarId'),
            'start_datetime': parse_datetime(event_data.get('startDateTime')),
            'end_datetime': parse_datetime(event_data.get('endDateTime')),
            'created_at_onedoc': parse_datetime(event_data.get('createdAt')),
            'updated_at_onedoc': parse_datetime(event_data.get('updatedAt')),
            'deleted_at_onedoc': parse_datetime(event_data.get('deletedAt')),
            'booked_online': event_data.get('bookedOnline'),
            'status': event_data.get('status'),
            'taken_by': event_data.get('takenBy'),
            
            # References
            'patient_ref': patient_ref,
            'location_ref': location_ref,
            'professional_ref': professional_ref,
            'profession_ref': profession_ref,
            'appointment_type_ref': appointment_type_ref,
            
            # Denormalized data for quick queries
            'patient_onedoc_id': patient_info.get('id'),
            'patient_name': patient_name,
            'patient_email': patient_info.get('email'),
            'location_onedoc_id': event_data.get('location', {}).get('id'),
            'location_name': event_data.get('location', {}).get('name'),
            'professional_onedoc_id': event_data.get('professional', {}).get('id'),
            'professional_name': event_data.get('professional', {}).get('name'),
            'profession_onedoc_id': event_data.get('profession', {}).get('id'),
            'profession_name': event_data.get('profession', {}).get('name'),
            'appointment_type_onedoc_id': event_data.get('appointmentType', {}).get('id'),
            'appointment_type_name': event_data.get('appointmentType', {}).get('name'),
            'appointment_duration_minutes': event_data.get('appointmentType', {}).get('durationMinutes'),
            
            # Metadata
            'original_payload': event_data,
            'received_at': request_start,
            'processed_at': datetime.utcnow(),
            'source': 'onedoc_webhook',
            'webhook_version': '2025.02',
            'server_info': {
                'user_agent': request.headers.get('User-Agent'),
                'remote_addr': request.remote_addr,
                'content_length': request.content_length
            }
        }
        
        # Handle deletion
        is_deleted = event_data.get('deletedAt') is not None
        if is_deleted:
            event_doc['is_deleted'] = True
            event_doc['deletion_detected_at'] = datetime.utcnow()
            logger.warning(f"🗑️ Event {event_id} marked as DELETED")
        else:
            event_doc['is_deleted'] = False
        
        # Remove None values
        event_doc = {k: v for k, v in event_doc.items() if v is not None}
        
        # Save main event
        result = safe_upsert(
            db_manager.get_collection('events'),
            {'onedoc_id': event_data.get('id')},
            event_doc,
            'Event',
            str(event_id)
        )
        
        processing_time = (datetime.utcnow() - request_start).total_seconds()
        
        logger.info(f"🎉 SUCCESS - Event {event_id} processed in {processing_time:.2f}s")
        
        return jsonify({
            'status': 'success',
            'message': 'OneDoc event processed successfully',
            'data': {
                'event_onedoc_id': event_id,
                'event_mongodb_id': str(result) if result else None,
                'patient_name': patient_name,
                'appointment_datetime': event_data.get('startDateTime'),
                'is_deleted': is_deleted,
                'processing_time_seconds': processing_time
            },
            'timestamp': datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        processing_time = (datetime.utcnow() - request_start).total_seconds()
        error_id = str(ObjectId())
        
        logger.error(f"💥 WEBHOOK ERROR - Event {event_id} | Error ID: {error_id}")
        logger.error(f"Error: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        return jsonify({
            'status': 'error',
            'message': 'Failed to process OneDoc event',
            'error': {
                'error_id': error_id,
                'type': type(e).__name__,
                'message': str(e),
                'event_id': event_id,
                'processing_time_seconds': processing_time
            },
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Comprehensive health check"""
    try:
        health_data = db_manager.health_check()
        health_data.update({
            'service': 'OneDoc Webhook Server',
            'version': '1.0.0',
            'environment': 'production',
            'timestamp': datetime.utcnow().isoformat()
        })
        
        status_code = 200 if health_data['status'] == 'healthy' else 503
        return jsonify(health_data), status_code
        
    except Exception as e:
        logger.error(f"💔 Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 503

@app.route('/stats', methods=['GET'])
@require_auth
def get_stats():
    """Detailed statistics endpoint"""
    try:
        events_collection = db_manager.get_collection('events')
        
        # Basic counts
        total_events = events_collection.count_documents({})
        deleted_events = events_collection.count_documents({'is_deleted': True})
        active_events = total_events - deleted_events
        
        # Recent activity
        recent_events = list(events_collection.find(
            {},
            {
                'onedoc_id': 1, 'patient_name': 1, 'start_datetime': 1,
                'status': 1, 'received_at': 1, 'is_deleted': 1
            }
        ).sort('received_at', -1).limit(10))
        
        # Convert dates for JSON serialization
        for event in recent_events:
            event['_id'] = str(event['_id'])
            for date_field in ['start_datetime', 'received_at']:
                if event.get(date_field):
                    event[date_field] = event[date_field].isoformat()
        
        stats = {
            'events': {
                'total': total_events,
                'active': active_events,
                'deleted': deleted_events
            },
            'entities': {
                name: db_manager.get_collection(name).count_documents({})
                for name in ['patients', 'locations', 'professionals', 'professions', 'appointment_types']
            },
            'recent_events': recent_events,
            'database_info': {
                'name': DATABASE_NAME,
                'cluster': 'Novapsy (MongoDB Atlas)',
                'status': 'connected'
            },
            'generated_at': datetime.utcnow().isoformat()
        }
        
        return jsonify(stats), 200
        
    except Exception as e:
        logger.error(f"📊 Stats error: {e}")
        return jsonify({
            'error': 'Failed to retrieve stats',
            'message': str(e)
        }), 500

@app.route('/test', methods=['POST'])
def test_endpoint():
    """Test endpoint (no auth required)"""
    try:
        data = request.get_json() or {}
        return jsonify({
            'status': 'test_success',
            'message': 'Webhook endpoint is reachable',
            'received_keys': list(data.keys()),
            'timestamp': datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'test_error',
            'error': str(e)
        }), 500

@app.route('/', methods=['GET'])
def root():
    """Root endpoint info"""
    return jsonify({
        'service': 'OneDoc Webhook Server',
        'status': 'running',
        'endpoints': {
            'webhook': '/events [POST] (requires auth)',
            'health': '/health [GET]',
            'stats': '/stats [GET] (requires auth)',
            'test': '/test [POST]'
        },
        'timestamp': datetime.utcnow().isoformat()
    })

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'error': 'Endpoint not found',
        'available_endpoints': ['/', '/events', '/health', '/stats', '/test']
    }), 404

@app.errorhandler(500)
def internal_error(error):
    error_id = str(ObjectId())
    logger.error(f"💥 Internal server error {error_id}: {error}")
    return jsonify({
        'error': 'Internal server error',
        'error_id': error_id
    }), 500

if __name__ == '__main__':
    logger.info("🚀 Starting OneDoc Webhook Server")
    logger.info(f"🌐 Port: {PORT}")
    logger.info(f"📊 Database: {DATABASE_NAME}")
    logger.info(f"🔐 Auth User: {WEBHOOK_USERNAME}")
    logger.info(f"📝 Log Level: {LOG_LEVEL}")
    
    # Create indexes on startup
    try:
        logger.info("🔧 Creating database indexes...")
        collections = db_manager.collections
        
        # Create indexes
        collections['events'].create_index('onedoc_id', unique=True, background=True)
        collections['events'].create_index('start_datetime', background=True)
        collections['events'].create_index('status', background=True)
        collections['events'].create_index('is_deleted', background=True)
        collections['events'].create_index('received_at', background=True)
        
        collections['patients'].create_index('onedoc_id', unique=True, background=True)
        collections['patients'].create_index('email', background=True)
        
        for collection_name in ['locations', 'professionals', 'professions', 'appointment_types']:
            collections[collection_name].create_index('onedoc_id', unique=True, background=True)
        
        logger.info("✅ Database indexes created")
    except Exception as e:
        logger.warning(f"⚠️ Index creation warning: {e}")
    
    app.run(host='0.0.0.0', port=PORT, debug=False)