const express = require('express');
const session = require('express-session');
const path = require('path');
const { 
  initializeDatabase, 
  authenticateUser, 
  sql,
  createNotification,
  getNotifications,
  markNotificationAsRead,
  markAllNotificationsAsRead,
  getUnreadNotificationCount,
  saveScanHistory,
  getScanHistory,
  clearScanHistory,
  deleteScanHistory
} = require('./database');
const {
  initializeInventoryTable,
  getAllInventoryItems,
  getInventoryItemById,
  createInventoryItem,
  updateInventoryItem,
  deleteInventoryItem,
  deleteMultipleInventoryItems,
  getAllCategories,
  getAllWarehouses,
  getInventoryStats,
  updateItemQuantity,
  // New order shipments
  getAllOrderShipments,
  getOrderShipmentById,
  createOrderShipment,
  updateOrderShipment,
  deleteOrderShipment,
  getOrderShipmentStats,
  updateOrderShipmentStatus,
  getRecentOrderShipmentActivity,
  getShippingZoneStats,
  getStockOverview,
  getStockByCategory,
  getStockByWarehouse,
  getAllZones,
  getZoneById,
  createZone,
  updateZone,
  deleteZone,
  getZoneStats,
  getZonesByWarehouse,
  optimizeWarehouseLayout,
  analyzeWarehouseTraffic,
  generateWarehouseHeatmap,
  exportWarehouseLayout
} = require('./inventory');

const {
  isDatabaseAvailable,
  getDatabaseStatus
} = require('./database');

require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname)));

// Session configuration
app.use(session({
  secret: process.env.SESSION_SECRET || 'fox-control-hub-secret-key',
  resave: false,
  saveUninitialized: false,
  cookie: {
    secure: false, // Set to true if using HTTPS
    maxAge: 24 * 60 * 60 * 1000 // 24 hours
  }
}));

// Authentication middleware
const requireAuth = (req, res, next) => {
  if (req.session && req.session.user) {
    return next();
  } else {
    return res.redirect('/loginpage.html');
  }
};

// Routes

// Root route - redirect to login if not authenticated, otherwise to inventory
app.get('/', (req, res) => {
  if (req.session && req.session.user) {
    res.redirect('/inventory.html');
  } else {
    res.redirect('/loginpage.html');
  }
});

// Login page
app.get('/login.html', (req, res) => {
  if (req.session && req.session.user) {
    res.redirect('/inventory.html');
  } else {
    res.sendFile(path.join(__dirname, 'loginpage.html'));
  }
});

app.get('/loginpage.html', (req, res) => {
  if (req.session && req.session.user) {
    res.redirect('/inventory.html');
  } else {
    res.sendFile(path.join(__dirname, 'loginpage.html'));
  }
});

// Login POST route
app.post('/login', async (req, res) => {
  const { username, password } = req.body;
  
  try {
    let user;
    try {
      user = await authenticateUser(username, password);
    } catch (dbError) {
      // Mock authentication if database is not available
      if (username === 'admin' && password === 'admin') {
        user = { id: 1, username: 'admin', role: 'admin' };
      }
    }
    
    if (user) {
      req.session.user = user;
      res.json({ success: true, message: 'Login successful' });
    } else {
      res.status(401).json({ success: false, message: 'Invalid username or password' });
    }
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ success: false, message: 'Server error' });
  }
});

// Logout route
app.post('/logout', (req, res) => {
  req.session.destroy((err) => {
    if (err) {
      return res.status(500).json({ success: false, message: 'Could not log out' });
    }
    res.json({ success: true, message: 'Logout successful' });
  });
});

// Protected routes - require authentication
app.get('/inventory.html', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'Inventory.html'));
});

app.get('/barcode.html', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'Barcode.html'));
});

// Material Shipments removed

app.get('/ordershipments.html', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'OrderShipments.html'));
});

app.get('/reports.html', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'Reports.html'));
});

app.get('/stocktracking.html', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'StockTracking.html'));
});

app.get('/warehouselayandopti.html', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'WarehouseLayAndOpti.html'));
});

// API route to get current user info
app.get('/api/user', requireAuth, (req, res) => {
  res.json({
    success: true,
    data: {
      username: req.session.user.username,
      role: req.session.user.role
    }
  });
});

// Notification API endpoints
app.get('/api/notifications', requireAuth, async (req, res) => {
  try {
    const notifications = await getNotifications(20);
    res.json({ success: true, data: notifications });
  } catch (error) {
    console.error('Get notifications error:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch notifications' });
  }
});

app.get('/api/notifications/unread-count', requireAuth, async (req, res) => {
  try {
    const count = await getUnreadNotificationCount();
    res.json({ 
      success: true, 
      count: count || 0 
    });
  } catch (error) {
    console.error('Get notification count error:', error);
    res.status(500).json({ 
      success: false, 
      message: 'Failed to fetch notification count',
      count: 0
    });
  }
});

app.post('/api/notifications/:id/read', requireAuth, async (req, res) => {
  try {
    const success = await markNotificationAsRead(req.params.id);
    if (success) {
      res.json({ success: true, message: 'Notification marked as read' });
    } else {
      res.status(500).json({ success: false, message: 'Failed to mark notification as read' });
    }
  } catch (error) {
    console.error('Mark notification as read error:', error);
    res.status(500).json({ success: false, message: 'Failed to mark notification as read' });
  }
});

app.post('/api/notifications/read-all', requireAuth, async (req, res) => {
  try {
    const success = await markAllNotificationsAsRead();
    if (success) {
      res.json({ success: true, message: 'All notifications marked as read' });
    } else {
      res.status(500).json({ success: false, message: 'Failed to mark all notifications as read' });
    }
  } catch (error) {
    console.error('Mark all notifications as read error:', error);
    res.status(500).json({ success: false, message: 'Failed to mark all notifications as read' });
  }
});

// Create new notification
app.post('/api/notifications', requireAuth, async (req, res) => {
  try {
    const { title, message, type = 'info' } = req.body;

    // Validate required fields
    if (!title || !message) {
      return res.status(400).json({
        success: false,
        message: 'Title and message are required'
      });
    }

    // Validate notification type
    const validTypes = ['info', 'success', 'warning', 'error'];
    if (!validTypes.includes(type)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid notification type'
      });
    }

    const notification = await createNotification(title, message, type);
    
    res.json({ 
      success: true, 
      message: 'Notification created successfully', 
      data: notification 
    });
  } catch (error) {
    console.error('Create notification error:', error);
    res.status(500).json({ 
      success: false, 
      message: 'Failed to create notification',
      error: error.message 
    });
  }
});

// API route to check database connection status (simplified)
app.get('/api/db-status', async (req, res) => {
  try {
    // First check if we have DATABASE_URL
    if (!process.env.DATABASE_URL) {
      return res.json({
        connected: false,
        error: 'DATABASE_URL not found in environment',
        timestamp: new Date().toISOString()
      });
    }

    // Use the enhanced database status functions instead of direct SQL
    const { getDatabaseStatus, isDatabaseAvailable } = require('./database');
    const dbStatus = getDatabaseStatus();
    const isAvailable = isDatabaseAvailable();
    
    let connectionInfo = null;
    if (isAvailable) {
      try {
        const sql = await require('./database').sql();
        if (sql) {
          // Simple query that's less likely to timeout
          const result = await sql`SELECT 1 as test`;
          connectionInfo = {
            test: result[0].test === 1,
            connected: true
          };
        } else {
          connectionInfo = {
            error: 'SQL connection not available',
            connected: false
          };
        }
      } catch (error) {
        connectionInfo = {
          error: error.message,
          connected: false
        };
      }
    }
    
    res.json({
      success: true,
      database: {
        available: isAvailable,
        status: dbStatus,
        connection: connectionInfo
      },
      environment: {
        hasDatabaseUrl: !!process.env.DATABASE_URL,
        nodeEnv: process.env.NODE_ENV || 'development'
      },
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Database status check failed:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to check database status',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// API: Get all inventory items
app.get('/api/inventory', requireAuth, async (req, res) => {
  try {
    const items = await getAllInventoryItems();
    res.json({ success: true, data: items });
  } catch (err) {
    console.error('Failed to fetch inventory items:', err);
    // Return an empty dataset to keep UI responsive, but include error message
    res.json({ success: true, data: [], warning: 'Database error while fetching inventory items' });
  }
});

app.get('/api/inventory/:id', requireAuth, async (req, res) => {
  try {
    const item = await getInventoryItemById(req.params.id);
    if (item) {
      res.json({ success: true, data: item });
    } else {
      res.status(404).json({ success: false, message: 'Item not found' });
    }
  } catch (err) {
    res.status(500).json({ success: false, message: 'Failed to fetch item' });
  }
});

// API: Insert new inventory item
app.post('/api/inventory', requireAuth, async (req, res) => {
  try {
    const newItem = await createInventoryItem(req.body);
    
    // Create notification for successful item addition
    try {
      await createNotification(
        'Item Added Successfully',
        `${newItem.name} has been added to inventory with SKU: ${newItem.sku}`,
        'success'
      );
    } catch (notifError) {
      console.error('Failed to create notification:', notifError);
    }
    
    res.json({ success: true, message: 'Item inserted successfully', data: newItem });
  } catch (err) {
    res.status(500).json({ success: false, message: 'Failed to insert item' });
  }
});

// API: Delete multiple inventory items
app.post('/api/inventory/delete-multiple', requireAuth, async (req, res) => {
  try {
    const { ids } = req.body;
    await deleteMultipleInventoryItems(ids);
    
    // Create notification for successful item deletion
    try {
      await createNotification(
        'Items Deleted Successfully',
        `${ids.length} item(s) have been deleted from inventory`,
        'warning'
      );
    } catch (notifError) {
      console.error('Failed to create notification:', notifError);
    }
    
    res.json({ success: true, message: 'Items deleted successfully' });
  } catch (err) {
    res.status(500).json({ success: false, message: 'Failed to delete items' });
  }
});

// API: Update inventory item
app.put('/api/inventory/:id', requireAuth, async (req, res) => {
  try {
    const updatedItem = await updateInventoryItem(req.params.id, req.body);
    
    // Create notification for successful item update
    try {
      await createNotification(
        'Item Updated Successfully',
        `${updatedItem.name} (SKU: ${updatedItem.sku}) has been updated in inventory`,
        'info'
      );
    } catch (notifError) {
      console.error('Failed to create notification:', notifError);
    }
    
    res.json({ success: true, message: 'Item updated successfully', data: updatedItem });
  } catch (err) {
    res.status(500).json({ success: false, message: 'Failed to update item' });
  }
});

// API: Get all categories
app.get('/api/categories', requireAuth, async (req, res) => {
  try {
    const categories = await getAllCategories();
    res.json({ 
      success: true, 
      data: categories || [] 
    });
  } catch (error) {
    console.error('Error fetching categories:', error);
    res.status(500).json({ 
      success: false, 
      message: 'Failed to fetch categories',
      error: error.message
    });
  }
});


// API: Get all warehouses
app.get('/api/warehouses', requireAuth, async (req, res) => {
  try {
    const warehouses = await getAllWarehouses();
    res.json({ success: true, data: warehouses });
  } catch (err) {
    res.status(500).json({ success: false, message: 'Failed to fetch warehouses' });
  }
});

// Stock Tracking APIs
app.get('/api/stock/overview', requireAuth, async (req, res) => {
  try {
    const threshold = parseInt(req.query.threshold) || 50;
    const data = await getStockOverview({ threshold });
    res.json({ success: true, data });
  } catch (err) {
    console.error('Stock overview error:', err);
    res.status(500).json({ success: false, message: 'Failed to fetch stock overview' });
  }
});

app.get('/api/stock/by-category', requireAuth, async (req, res) => {
  try {
    const rows = await getStockByCategory();
    res.json({ success: true, data: rows });
  } catch (err) {
    console.error('Stock by category error:', err);
    res.status(500).json({ success: false, message: 'Failed to fetch stock by category' });
  }
});

app.get('/api/stock/by-warehouse', requireAuth, async (req, res) => {
  try {
    const rows = await getStockByWarehouse();
    res.json({ success: true, data: rows });
  } catch (err) {
    console.error('Stock by warehouse error:', err);
    res.status(500).json({ success: false, message: 'Failed to fetch stock by warehouse' });
  }
});

// Products and Pricing (read-only)
app.get('/api/products', requireAuth, async (req, res) => {
  try {
    const db = require('./database');
    const sqlConn = await db.sql();
    const rows = await sqlConn`SELECT product_id, product_name, product_description, product_category, product_image FROM products ORDER BY product_id`;
    res.json({ success: true, data: rows });
  } catch (error) {
    console.error('Fetch products error:', error);
    res.json({ success: true, data: [] });
  }
});

app.get('/api/product-pricing/:productId', requireAuth, async (req, res) => {
  try {
    const db = require('./database');
    const sqlConn = await db.sql();
    const rows = await sqlConn`
      SELECT product_id, price, discount_rate, effective_date
      FROM product_pricing
      WHERE product_id = ${req.params.productId}
      ORDER BY effective_date DESC
      LIMIT 1
    `;
    res.json({ success: true, data: rows[0] || null });
  } catch (error) {
    console.error('Fetch product pricing error:', error);
    res.json({ success: true, data: null });
  }
});

// API: Get scan history
app.get('/api/scan-history', requireAuth, async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 100;
    
    // Validate limit
    if (isNaN(limit) || limit < 1) {
      return res.status(400).json({
        success: false,
        message: 'Invalid limit parameter'
      });
    }

    const history = await getScanHistory(limit);
    
    res.json({ 
      success: true, 
      data: history,
      count: history.length,
      limit: limit
    });
  } catch (err) {
    console.error('Fetch scan history error:', err);
    res.status(500).json({ 
      success: false, 
      message: 'Failed to fetch scan history',
      error: err.message 
    });
  }
});

// API: Save scan to history
app.post('/api/scan-history', requireAuth, async (req, res) => {
  try {
    // Validate required fields
    const { code, type } = req.body;
    if (!code) {
      return res.status(400).json({
        success: false,
        message: 'Scan code is required'
      });
    }

    // Add user info to scan data
    const scanData = {
      ...req.body,
      scannedBy: req.session.user.username,
      notes: req.body.notes || `Scanned by ${req.session.user.username}`
    };

    const savedScan = await saveScanHistory(scanData);
    
    if (savedScan) {
      // Create notification for successful scan
      await createNotification(
        'New Scan Recorded',
        `New ${scanData.type || 'barcode'} scan: ${scanData.code}`,
        'info'
      );
      
      res.json({ 
        success: true, 
        message: 'Scan saved successfully', 
        data: savedScan 
      });
    } else {
      throw new Error('Failed to save scan to database');
    }
  } catch (err) {
    console.error('Save scan error:', err);
    res.status(500).json({ 
      success: false, 
      message: 'Failed to save scan',
      error: err.message 
    });
  }
});

// API: Clear scan history
app.delete('/api/scan-history', requireAuth, async (req, res) => {
  try {
    const success = await clearScanHistory();
    
    if (success) {
      // Create notification for successful clear
      await createNotification(
        'Scan History Cleared',
        'All scan history records have been cleared',
        'warning'
      );
      
      res.json({ 
        success: true, 
        message: 'Scan history cleared successfully' 
      });
    } else {
      res.status(500).json({ 
        success: false, 
        message: 'Failed to clear scan history' 
      });
    }
  } catch (err) {
    console.error('Clear scan history error:', err);
    res.status(500).json({ 
      success: false, 
      message: 'Failed to clear scan history',
      error: err.message 
    });
  }
});

// API: Delete individual scan history record
app.delete('/api/scan-history/:id', requireAuth, async (req, res) => {
  try {
    const scanId = parseInt(req.params.id);
    
    if (isNaN(scanId)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid scan ID format'
      });
    }

    const deleted = await deleteScanHistory(scanId);
    
    if (deleted) {
      // Create notification for successful deletion
      await createNotification(
        'Scan Deleted',
        `Scan record ${scanId} has been deleted`,
        'info'
      );
      
      res.json({
        success: true,
        message: 'Scan deleted successfully'
      });
    } else {
      res.status(404).json({
        success: false,
        message: 'Scan record not found'
      });
    }
  } catch (error) {
    console.error('Delete scan error:', error);
    res.status(500).json({ 
      success: false,
      message: 'Failed to delete scan',
      error: error.message
    });
  }
});

// Test route to get all inventory items without authentication
app.get('/api/test-inventory', async (req, res) => {
  try {
    const items = await getAllInventoryItems();
    res.json(items);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Initialize database and start server
const startServer = async () => {
  try {
    // Try to initialize database, but don't fail if it doesn't work
    try {
      await initializeDatabase();
      // Check if database is actually available
      const { isDatabaseAvailable } = require('./database');
      if (isDatabaseAvailable()) {
        console.log(`🗄️ Database: Connected to Neon PostgreSQL`);
      } else {
        console.log(`⚠️ Database initialization completed but connection not available`);
      }
    } catch (dbError) {
      console.log(`⚠️ Database connection failed, running in mock mode`);
    }
    
    app.listen(PORT, () => {
      console.log(`🚀 Fox Control Hub server running on http://localhost:${PORT}`);
      console.log(`📝 Default login credentials:`);
      console.log(`   Username: admin`);
      console.log(`   Password: admin`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
};

// Material Shipments endpoints removed

// Order Shipments API endpoints
app.get('/api/order-shipments/stats', requireAuth, async (req, res) => {
  try {
    const stats = await getOrderShipmentStats();
    res.json({ success: true, data: stats });
  } catch (error) {
    console.error('Error fetching order shipment stats:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch order shipment statistics' });
  }
});

app.get('/api/order-shipments', requireAuth, async (req, res) => {
  try {
    const { search, status, priority, date } = req.query;
    const filters = { search, status, priority, date };
    const orders = await getAllOrderShipments(filters);
    res.json({ success: true, data: orders || [], count: orders ? orders.length : 0 });
  } catch (error) {
    console.error('Error fetching order shipments:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch order shipments' });
  }
});

app.get('/api/order-shipments/:id(\\d+)', requireAuth, async (req, res) => {
  try {
    const order = await getOrderShipmentById(req.params.id);
    if (order) {
      res.json({ success: true, data: order });
    } else {
      res.status(404).json({ success: false, message: 'Order not found' });
    }
  } catch (error) {
    console.error('Error fetching order shipment:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch order' });
  }
});

app.post('/api/order-shipments', requireAuth, async (req, res) => {
  try {
    const newOrder = await createOrderShipment(req.body);
    res.json({ success: true, message: 'Order created successfully', data: newOrder });
  } catch (error) {
    console.error('Error creating order shipment:', error);
    res.status(500).json({ success: false, message: 'Failed to create order' });
  }
});

app.put('/api/order-shipments/:id(\\d+)', requireAuth, async (req, res) => {
  try {
    const updatedOrder = await updateOrderShipment(req.params.id, req.body);
    if (updatedOrder) {
      res.json({ success: true, message: 'Order updated successfully', data: updatedOrder });
    } else {
      res.status(404).json({ success: false, message: 'Order not found' });
    }
  } catch (error) {
    console.error('Error updating order shipment:', error);
    res.status(500).json({ success: false, message: 'Failed to update order' });
  }
});

app.delete('/api/order-shipments/:id(\\d+)', requireAuth, async (req, res) => {
  try {
    const deleted = await deleteOrderShipment(req.params.id);
    if (deleted) {
      res.json({ success: true, message: 'Order deleted successfully' });
    } else {
      res.status(404).json({ success: false, message: 'Order not found' });
    }
  } catch (error) {
    console.error('Error deleting order shipment:', error);
    res.status(500).json({ success: false, message: 'Failed to delete order' });
  }
});

app.post('/api/order-shipments/:id(\\d+)/status', requireAuth, async (req, res) => {
  try {
    const { status, setShipDate, setDeliveryDate } = req.body;
    const updated = await updateOrderShipmentStatus(req.params.id, status, { setShipDate, setDeliveryDate });
    if (updated) {
      res.json({ success: true, message: 'Order status updated', data: updated });
    } else {
      res.status(404).json({ success: false, message: 'Order not found' });
    }
  } catch (error) {
    console.error('Error updating order status:', error);
    const message = (error && error.message) ? error.message : 'Failed to update order status';
    const isBusinessError = /you cannot ship the item|Order not found/i.test(message);
    res.status(isBusinessError ? 400 : 500).json({ success: false, message });
  }
});

app.get('/api/order-shipments/recent-activity', requireAuth, async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(50, parseInt(req.query.limit || '10')));
    const rows = await getRecentOrderShipmentActivity(limit);
    res.json({ success: true, data: rows });
  } catch (error) {
    console.error('Error fetching recent activity:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch recent activity' });
  }
});

// Shipping zone statistics endpoint
app.get('/api/shipping-zone-stats', requireAuth, async (req, res) => {
  try {
    const stats = await getShippingZoneStats();
    res.json({ success: true, data: stats });
  } catch (error) {
    console.error('Error fetching shipping zone stats:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch shipping zone statistics' });
  }
});

// Zone management endpoints
app.get('/api/zones', requireAuth, async (req, res) => {
  try {
    const { search, warehouse_id, zone_type, status } = req.query;
    const filters = { search, warehouse_id, zone_type, status };
    const zones = await getAllZones(filters);
    res.json({ success: true, data: zones || [], count: zones ? zones.length : 0 });
  } catch (error) {
    console.error('Error fetching zones:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch zones' });
  }
});

app.get('/api/zones/:zoneId', requireAuth, async (req, res) => {
  try {
    const zone = await getZoneById(req.params.zoneId);
    if (zone) {
      res.json({ success: true, data: zone });
    } else {
      res.status(404).json({ success: false, message: 'Zone not found' });
    }
  } catch (error) {
    console.error('Error fetching zone:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch zone' });
  }
});

app.post('/api/zones', requireAuth, async (req, res) => {
  try {
    const newZone = await createZone(req.body);
    res.json({ success: true, message: 'Zone created successfully', data: newZone });
  } catch (error) {
    console.error('Error creating zone:', error);
    res.status(500).json({ success: false, message: 'Failed to create zone' });
  }
});

app.put('/api/zones/:zoneId', requireAuth, async (req, res) => {
  try {
    const updatedZone = await updateZone(req.params.zoneId, req.body);
    if (updatedZone) {
      res.json({ success: true, message: 'Zone updated successfully', data: updatedZone });
    } else {
      res.status(404).json({ success: false, message: 'Zone not found' });
    }
  } catch (error) {
    console.error('Error updating zone:', error);
    res.status(500).json({ success: false, message: 'Failed to update zone' });
  }
});

app.delete('/api/zones/:zoneId', requireAuth, async (req, res) => {
  try {
    const deleted = await deleteZone(req.params.zoneId);
    if (deleted) {
      res.json({ success: true, message: 'Zone deleted successfully' });
    } else {
      res.status(404).json({ success: false, message: 'Zone not found' });
    }
  } catch (error) {
    console.error('Error deleting zone:', error);
    res.status(500).json({ success: false, message: 'Failed to delete zone' });
  }
});

app.get('/api/zones/stats', requireAuth, async (req, res) => {
  try {
    const stats = await getZoneStats();
    res.json({ success: true, data: stats });
  } catch (error) {
    console.error('Error fetching zone stats:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch zone statistics' });
  }
});

app.get('/api/zones/warehouse/:warehouseId', requireAuth, async (req, res) => {
  try {
    const zones = await getZonesByWarehouse(req.params.warehouseId);
    res.json({ success: true, data: zones || [], count: zones ? zones.length : 0 });
  } catch (error) {
    console.error('Error fetching zones by warehouse:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch zones by warehouse' });
  }
});

// Warehouse layout optimization endpoint
app.post('/api/warehouses/:warehouseId/optimize', requireAuth, async (req, res) => {
  try {
    const result = await optimizeWarehouseLayout(req.params.warehouseId);
    res.json(result);
  } catch (error) {
    console.error('Error optimizing warehouse layout:', error);
    res.status(500).json({ success: false, message: error.message });
  }
});

// Warehouse traffic analysis endpoint
app.post('/api/warehouses/:warehouseId/analyze-traffic', requireAuth, async (req, res) => {
  try {
    const result = await analyzeWarehouseTraffic(req.params.warehouseId);
    res.json(result);
  } catch (error) {
    console.error('Error analyzing warehouse traffic:', error);
    res.status(500).json({ success: false, message: error.message });
  }
});

// Warehouse heatmap generation endpoint
app.post('/api/warehouses/:warehouseId/heatmap', requireAuth, async (req, res) => {
  try {
    const { heatmapType = 'utilization' } = req.body;
    const result = await generateWarehouseHeatmap(req.params.warehouseId, heatmapType);
    res.json(result);
  } catch (error) {
    console.error('Error generating warehouse heatmap:', error);
    res.status(500).json({ success: false, message: error.message });
  }
});

// Warehouse layout export endpoint
app.post('/api/warehouses/:warehouseId/export', requireAuth, async (req, res) => {
  try {
    const { exportFormat = 'json' } = req.body;
    const result = await exportWarehouseLayout(req.params.warehouseId, exportFormat);
    res.json(result);
  } catch (error) {
    console.error('Error exporting warehouse layout:', error);
    res.status(500).json({ success: false, message: error.message });
  }
});

// Warehouses endpoint
app.get('/api/warehouses', requireAuth, async (req, res) => {
  try {
    const warehouses = await getAllWarehouses();
    res.json({ success: true, data: warehouses || [], count: warehouses ? warehouses.length : 0 });
  } catch (error) {
    console.error('Error fetching warehouses:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch warehouses' });
  }
});

app.get('/api/health/db', requireAuth, async (req, res) => {
  try {
    const db = require('./database');
    const conn = await db.sql();
    const r = await conn`SELECT 1 as ok`;
    res.json({ ok: true, result: r[0]?.ok === 1 });
  } catch (e) {
    res.json({ ok: false, error: e?.message });
  }
});



startServer();

