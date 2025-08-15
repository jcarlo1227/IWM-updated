const { neon } = require('@neondatabase/serverless');
require('dotenv').config();

// Material shipments removed

let sql = null;
let connectionPromise = null;

// Initialize database connection
async function initializeConnection() {
  if (!connectionPromise) {
    connectionPromise = (async () => {
      if (process.env.DATABASE_URL) {
        try {
          console.log('🔌 Attempting to connect to database...');
          console.log('📡 Database URL format check:', process.env.DATABASE_URL.includes('neon') ? 'Neon format detected' : 'Unknown format');
          
          const connection = neon(process.env.DATABASE_URL);
          
          // For Neon databases, add initial delay to allow startup
          if (process.env.DATABASE_URL.includes('neon')) {
            console.log('🌅 Neon database detected, allowing startup time...');
            await new Promise(resolve => setTimeout(resolve, 3000)); // Wait 3 seconds for Neon to be ready
          }
          
          // Test connection with timeout and retry logic
          let retries = 5;
          let lastError = null;
          let delay = 1000; // Start with 1 second delay
          
          while (retries > 0) {
            try {
              console.log(`🔄 Connection attempt ${6 - retries}/5...`);
              await connection`SELECT 1`;
              sql = connection;
              module.exports.sql = async () => connection;
              console.log('✅ Database connection initialized successfully');
              return connection;
            } catch (testError) {
              lastError = testError;
              retries--;
              if (retries > 0) {
                console.log(`⚠️ Connection test failed, retrying in ${delay/1000} seconds... (${retries} attempts left)`);
                console.log(`💡 Error: ${testError.message}`);
                await new Promise(resolve => setTimeout(resolve, delay));
                delay = Math.min(delay * 1.5, 5000); // Increase delay up to 5 seconds
              }
            }
          }
          
          // All retries failed
          console.error('❌ Connection test failed after all retries:', lastError.message);
          throw lastError;
          
        } catch (error) {
          console.error('⚠️ Database connection failed:');
          console.error('Error type:', error.name);
          console.error('Error message:', error.message);
          if (error.code) console.error('Error code:', error.code);
          if (error.stack) console.error('Stack trace:', error.stack);
          
          // Check if it's a network-related error
          if (error.message.includes('fetch failed') || error.message.includes('network') || error.message.includes('timeout')) {
            console.log('💡 This appears to be a network connectivity issue.');
            console.log('💡 Please check your internet connection and try again.');
            console.log('💡 If using Neon database, ensure the database is accessible from your network.');
            console.log('💡 Check if your firewall or antivirus is blocking the connection.');
            console.log('💡 Try accessing the database from a different network.');
            
            // Specific Neon database "fetch failed" troubleshooting
            if (error.message.includes('fetch failed') && process.env.DATABASE_URL.includes('neon')) {
              console.log('💡 Neon "fetch failed" specific solutions:');
              console.log('💡 1. This often happens during cold starts - wait a few minutes and retry');
              console.log('💡 2. Check if your Neon database is in "Idle" state in the dashboard');
              console.log('💡 3. Try accessing the database from Neon dashboard to wake it up');
              console.log('💡 4. Check if you have IP restrictions enabled that might block your connection');
              console.log('💡 5. Verify your DATABASE_URL is correct and includes the right password');
            }
          }
          
          // Check for common Neon database issues
          if (process.env.DATABASE_URL.includes('neon')) {
            console.log('💡 Neon database troubleshooting:');
            console.log('💡 1. Check if your Neon database is active in the dashboard');
            console.log('💡 2. Verify the connection string is correct');
            console.log('💡 3. Check if IP restrictions are enabled');
            console.log('💡 4. Ensure the database password is correct');
          }
          
          sql = null;
        }
      } else {
        console.log('⚠️ No DATABASE_URL found, running in mock mode');
        console.log('💡 To enable database functionality, set the DATABASE_URL environment variable');
        console.log('💡 Example: DATABASE_URL=postgresql://user:password@host:port/database');
        sql = null;
      }
      return sql;
    })();
  }
  return connectionPromise;
}

// Get SQL connection
async function getSql() {
  if (!sql) {
    try {
      await initializeConnection();
    } catch (error) {
      console.log('⚠️ Failed to initialize database connection, returning null');
      return null;
    }
  }
  return sql;
}

// Check if database is available
function isDatabaseAvailable() {
  return sql !== null;
}

// Check if database is actually working (can execute queries)
async function isDatabaseWorking() {
  if (!sql) {
    return false;
  }
  
  try {
    await sql`SELECT 1`;
    return true;
  } catch (error) {
    console.log('⚠️ Database connection test failed:', error.message);
    return false;
  }
}

// Get database status
function getDatabaseStatus() {
  return {
    available: isDatabaseAvailable(),
    connectionPromise: connectionPromise !== null,
    hasUrl: !!process.env.DATABASE_URL
  };
}

// Test database connection and create tables
const testConnection = async () => {
  const connection = await getSql();
  if (!connection) {
    console.log('⚠️ Database not available, skipping table creation');
    return;
  }
  
  try {
    const result = await connection`SELECT version()`;
    console.log('✅ Connected to Neon database successfully');
    console.log(`Database version: ${result[0].version}`);
    
    // Create users table if it doesn't exist
    await connection`
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(50) UNIQUE NOT NULL,
        password VARCHAR(255) NOT NULL,
        role VARCHAR(20) DEFAULT 'user',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;
    
    // Create notifications table if it doesn't exist
    await connection`
      CREATE TABLE IF NOT EXISTS notifications (
        id SERIAL PRIMARY KEY,
        title VARCHAR(100) NOT NULL,
        message TEXT NOT NULL,
        type VARCHAR(20) DEFAULT 'info',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_read BOOLEAN DEFAULT false
      )
    `;
    
    // Create scan history table if it doesn't exist
    await connection`
      CREATE TABLE IF NOT EXISTS scan_history (
        id SERIAL PRIMARY KEY,
        scanned_code VARCHAR(100) NOT NULL,
        scan_type VARCHAR(20) DEFAULT 'barcode',
        item_id INTEGER,
        product_name VARCHAR(255),
        quantity INTEGER DEFAULT 1,
        scan_status VARCHAR(20) DEFAULT 'found',
        scanned_by VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        notes TEXT
      )
    `;
    
    console.log('✅ Users, notifications, and scan history tables created/verified');
  } catch (err) {
    console.error('❌ Database connection error:', err);
  }
};

// Initialize database
const initializeDatabase = async () => {
  try {
    await testConnection();
    
    // Get the SQL connection
    const connection = await getSql();
    if (!connection) {
      console.log('⚠️ Database not available, skipping database initialization');
      return;
    }
    
    // Check if admin user exists, if not create it
    const adminCheck = await connection`SELECT * FROM users WHERE username = 'admin'`;
    
    if (adminCheck.length === 0) {
      const bcrypt = require('bcryptjs');
      const hashedPassword = await bcrypt.hash('admin', 10);
      
      await connection`
        INSERT INTO users (username, password, role) 
        VALUES ('admin', ${hashedPassword}, 'admin')
      `;
      
      console.log('✅ Default admin user created (username: admin, password: admin)');
    } else {
      console.log('✅ Admin user already exists');
    }
    
    // Initialize inventory tables
    const { initializeInventoryTable, initializeOrderShipmentsTable, getAllOrderShipments } = require('./inventory');
    await initializeInventoryTable();
    await initializeOrderShipmentsTable();
    // Trigger initial sync from production_planning (processed) into order_shipments
    try { 
      await getAllOrderShipments({}); 
      console.log('✅ Initial sync from production_planning completed');
    } catch (e) { 
      console.warn('⚠️ Initial sync from production_planning skipped:', e?.message); 
    }
    
  } catch (err) {
    console.error('❌ Database initialization error:', err);
  }
};

// User authentication functions
const authenticateUser = async (username, password) => {
  try {
    // Check if database is available
    if (!sql) {
      console.log('⚠️ Database not available, using mock authentication');
      // Mock authentication for testing when database is not available
      if (username === 'admin' && password === 'admin') {
        return {
          id: 1,
          username: 'admin',
          role: 'admin'
        };
      }
      return null;
    }
    
    const result = await sql`SELECT * FROM users WHERE username = ${username}`;
    
    if (result.length === 0) {
      return null;
    }
    
    const user = result[0];
    const bcrypt = require('bcryptjs');
    const isValidPassword = await bcrypt.compare(password, user.password);
    
    if (isValidPassword) {
      return {
        id: user.id,
        username: user.username,
        role: user.role
      };
    }
    
    return null;
  } catch (err) {
    console.error('Authentication error:', err);
    // Mock authentication for testing when database is not available
    if (username === 'admin' && password === 'admin') {
      return {
        id: 1,
        username: 'admin',
        role: 'admin'
      };
    }
    return null;
  }
};

// In-memory notifications storage for testing when database is not available
let inMemoryNotifications = [];
let nextNotificationId = 1;

// Notification functions
const createNotification = async (title, message, type = 'info') => {
  try {
    const result = await sql`
      INSERT INTO notifications (title, message, type, created_at, is_read) 
      VALUES (${title}, ${message}, ${type}, CURRENT_TIMESTAMP, false)
      RETURNING id, title, message, type, created_at, is_read
    `;
    return result[0];
  } catch (err) {
    console.error('Create notification error:', err);
    // Fallback to in-memory storage
    const notification = {
      id: nextNotificationId++,
      title,
      message,
      type,
      created_at: new Date().toISOString(),
      is_read: false
    };
    inMemoryNotifications.unshift(notification);
    // Keep only the latest 20 notifications
    if (inMemoryNotifications.length > 20) {
      inMemoryNotifications = inMemoryNotifications.slice(0, 20);
    }
    return notification;
  }
};

const getNotifications = async (limit = 10) => {
  try {
    const result = await sql`
      SELECT id, title, message, type, created_at, is_read 
      FROM notifications 
      ORDER BY created_at DESC 
      LIMIT ${limit}
    `;
    return result;
  } catch (err) {
    console.error('Get notifications error:', err);
    // Fallback to in-memory storage
    return inMemoryNotifications.slice(0, limit);
  }
};

const markNotificationAsRead = async (id) => {
  try {
    await sql`UPDATE notifications SET is_read = true WHERE id = ${id}`;
    return true;
  } catch (err) {
    console.error('Mark notification as read error:', err);
    // Fallback to in-memory storage
    const notification = inMemoryNotifications.find(n => n.id == id);
    if (notification) {
      notification.is_read = true;
      return true;
    }
    return false;
  }
};

const markAllNotificationsAsRead = async () => {
  try {
    await sql`UPDATE notifications SET is_read = true WHERE is_read = false`;
    return true;
  } catch (err) {
    console.error('Mark all notifications as read error:', err);
    // Fallback to in-memory storage
    inMemoryNotifications.forEach(notification => {
      notification.is_read = true;
    });
    return true;
  }
};

const getUnreadNotificationCount = async () => {
  try {
    const result = await sql`SELECT COUNT(*) as count FROM notifications WHERE is_read = false`;
    return result[0].count;
  } catch (err) {
    console.error('Get unread notification count error:', err);
    // Fallback to in-memory storage
    return inMemoryNotifications.filter(n => !n.is_read).length;
  }
};

// Scan history functions
const saveScanHistory = async (scanData) => {
  const connection = await getSql();
  if (!connection) {
    console.log('⚠️ Database not available, scan history saved locally only');
    return { id: Date.now(), created_at: new Date() };
  }
  
  try {
    // Validate required fields
    if (!scanData.code) {
      throw new Error('Scan code is required');
    }

    const result = await connection`
      INSERT INTO scan_history (
        scanned_code, scan_type, item_id, product_name, 
        quantity, scan_status, scanned_by, notes
      ) VALUES (
        ${scanData.code}, 
        ${scanData.type || 'barcode'}, 
        ${scanData.itemId || null}, 
        ${scanData.productName || null}, 
        ${scanData.quantity || 1}, 
        ${scanData.status || 'scanned'}, 
        ${scanData.scannedBy || 'unknown'}, 
        ${scanData.notes || null}
      ) RETURNING id, created_at, scanned_code, scan_type, product_name, quantity, scan_status
    `;
    
    console.log('Scan saved successfully:', result[0]);
    return result[0];
  } catch (err) {
    console.error('Save scan history error:', err);
    throw new Error(`Failed to save scan: ${err.message}`);
  }
};

const getScanHistory = async (limit = 100) => {
  if (!sql) {
    console.log('⚠️ Database not available, returning empty scan history');
    return [];
  }
  
  try {
    const result = await sql`
      SELECT * FROM scan_history 
      ORDER BY created_at DESC 
      LIMIT ${limit}
    `;
    return result;
  } catch (err) {
    console.error('Get scan history error:', err);
    return [];
  }
};

const clearScanHistory = async () => {
  if (!sql) {
    console.log('⚠️ Database not available, scan history cleared locally only');
    return true;
  }
  
  try {
    await sql`DELETE FROM scan_history`;
    return true;
  } catch (err) {
    console.error('Clear scan history error:', err);
    return false;
  }
};

const deleteScanHistory = async (scanId) => {
  if (!sql) {
    console.log('⚠️ Database not available, scan history deleted locally only');
    return true;
  }

  try {
    await sql`DELETE FROM scan_history WHERE id = ${scanId}`;
    // PostgreSQL DELETE doesn't return affected rows by default
    const check = await sql`SELECT EXISTS(SELECT 1 FROM scan_history WHERE id = ${scanId})`;
    return !check[0].exists; // Returns true if the row no longer exists
  } catch (err) {
    console.error('Delete scan history error:', err);
    throw err; // Propagate error to handle it in the API
  }
};


// Test database connection manually (for debugging)
async function testDatabaseConnection() {
  console.log('🧪 Testing database connection...');
  console.log('Environment variables:');
  console.log('- DATABASE_URL:', process.env.DATABASE_URL ? 'Set' : 'Not set');
  console.log('- NODE_ENV:', process.env.NODE_ENV || 'Not set');
  
  if (process.env.DATABASE_URL) {
    console.log('- DATABASE_URL format:', process.env.DATABASE_URL.includes('neon') ? 'Neon' : 'Other');
    console.log('- DATABASE_URL length:', process.env.DATABASE_URL.length);
    console.log('- DATABASE_URL starts with:', process.env.DATABASE_URL.substring(0, 20) + '...');
  }
  
  try {
    const connection = await getSql();
    if (connection) {
      console.log('✅ Database connection successful');
      try {
        const result = await connection`SELECT version()`;
        console.log('Database version:', result[0].version);
        return true;
      } catch (queryError) {
        console.log('⚠️ Connection object exists but query failed:', queryError.message);
        return false;
      }
    } else {
      console.log('❌ Database connection failed - no connection returned');
      return false;
    }
  } catch (error) {
    console.error('❌ Database connection test failed:', error);
    console.error('Error details:', {
      name: error.name,
      message: error.message,
      stack: error.stack
    });
    return false;
  }
}

module.exports = {
  sql: async () => {
    if (!sql) {
      await initializeConnection();
    }
    return sql;
  },
  testConnection,
  initializeDatabase,
  isDatabaseAvailable,
  isDatabaseWorking,
  getDatabaseStatus,
  testDatabaseConnection,
  authenticateUser,
  createNotification,
  getNotifications,
  markNotificationAsRead,
  markAllNotificationsAsRead,
  getUnreadNotificationCount,
  saveScanHistory,
  getScanHistory,
  clearScanHistory,
  deleteScanHistory
};
