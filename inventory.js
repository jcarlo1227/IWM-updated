const database = require('./database');

// Initialize inventory table
const initializeInventoryTable = async () => {
  try {
    const sql = await database.sql();
    await sql`
      CREATE TABLE IF NOT EXISTS inventory_items (
        id SERIAL PRIMARY KEY,
        item_code VARCHAR(50) UNIQUE NOT NULL,
        product_name VARCHAR(255) NOT NULL,
        unit_of_measure VARCHAR(10) NOT NULL,
        location VARCHAR(255) NOT NULL,
        category_id VARCHAR(50),
        status VARCHAR(20),
        warehouse_id VARCHAR(50),
        total_quantity INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;
    // Remove legacy columns and add new pricing columns
    await sql`ALTER TABLE inventory_items DROP COLUMN IF EXISTS buy_price`;
    await sql`ALTER TABLE inventory_items DROP COLUMN IF EXISTS sell_price`;
    await sql`ALTER TABLE inventory_items ADD COLUMN IF NOT EXISTS price DECIMAL(10,2)`;
    await sql`ALTER TABLE inventory_items ADD COLUMN IF NOT EXISTS product_id INTEGER`;
    await sql`ALTER TABLE inventory_items ADD COLUMN IF NOT EXISTS product_pricing_id INTEGER`;
    // Indexes
    await sql`CREATE INDEX IF NOT EXISTS idx_inventory_items_product_pricing_id ON inventory_items(product_pricing_id)`;
    // Determine primary key column of product_pricing for robust FK
    try {
      await sql`ALTER TABLE inventory_items DROP CONSTRAINT IF EXISTS inventory_items_product_pricing_id_fkey`;
    } catch (e) {
      // Ignore if constraint not present
    }
    
    console.log('✅ Inventory table created/verified');
    
    // Create categories table
    await sql`
      CREATE TABLE IF NOT EXISTS categories (
        id SERIAL PRIMARY KEY,
        category_id VARCHAR(50) UNIQUE NOT NULL,
        category_name VARCHAR(255) NOT NULL,
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;
    
    console.log('✅ Categories table created/verified');
    
    // Create warehouses table
    await sql`
      CREATE TABLE IF NOT EXISTS warehouses (
        id SERIAL PRIMARY KEY,
        warehouse_id VARCHAR(50) UNIQUE NOT NULL,
        warehouse_name VARCHAR(255) NOT NULL,
        location VARCHAR(255),
        capacity INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;
    
    console.log('✅ Warehouses table created/verified');
    
    
  } catch (err) {
    console.error('❌ Error creating inventory tables:', err);
    throw err;
  }

  try {
    // Insert default Categories
    const sql = await database.sql();
    await sql`
      INSERT INTO categories (category_id, category_name) VALUES
        ('CAT001', 'Materials'),
        ('CAT002', 'Products')
      ON CONFLICT (category_id) DO NOTHING;
    `;
    console.log('✅ Categories inserted');

    // Insert default Warehouses
    await sql`
      INSERT INTO warehouses (warehouse_id, warehouse_name) VALUES
        ('WH001', 'Main Warehouse'),
        ('WH002', 'Secondary Warehouse')
      ON CONFLICT (warehouse_id) DO NOTHING;
    `;
    console.log('✅ Warehouse inserted');
  } catch (err) {
    console.error('❌ Error inserting default data:', err);
    throw err;
  }
};

// Initialize material shipments table
const initializeMaterialShipmentsTable = async () => {
  try {
    const sql = await database.sql();
    await sql`
      CREATE TABLE IF NOT EXISTS material_shipments (
        id SERIAL PRIMARY KEY,
        shipment_id VARCHAR(50) UNIQUE NOT NULL,
        bom_id VARCHAR(50), 
        category_id VARCHAR(50) NOT NULL,
        material_name VARCHAR(255) NOT NULL,
        quantity INTEGER NOT NULL,
        unit VARCHAR(20) NOT NULL,
        shipment_type VARCHAR(20) NOT NULL,
        source VARCHAR(255) NOT NULL,
        destination VARCHAR(255) NOT NULL,
        status VARCHAR(20),
        date_shipped DATE,
        estimated_delivery DATE,
        received_date DATE,
        handled_by VARCHAR(100),
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;  
    console.log('✅ Material shipments table created/verified');
  } catch (err) {
    console.error('❌ Error creating material shipments table:', err);
    throw err;
  }
};

// Initialize order shipments table
const initializeOrderShipmentsTable = async () => {
  try {
    const sql = await database.sql();
    await sql`
      CREATE TABLE IF NOT EXISTS order_shipments (
        id SERIAL PRIMARY KEY,
        order_id VARCHAR(50) UNIQUE NOT NULL,
        customer_name VARCHAR(255) NOT NULL,
        item_code VARCHAR(50) NOT NULL,
        product_name VARCHAR(255) NOT NULL,
        quantity INTEGER NOT NULL,
        total_value DECIMAL(12,2) NOT NULL,
        priority VARCHAR(20) DEFAULT 'medium',
        status VARCHAR(20) DEFAULT 'processing',
        order_date DATE,
        ship_date DATE,
        delivery_date DATE,
        tracking_number VARCHAR(100),
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;
    console.log('✅ Order shipments table created/verified');
  } catch (err) {
    console.error('❌ Error creating order shipments table:', err);
    throw err;
  }
};

// Get all inventory items with optional filters
const getAllInventoryItems = async (filters = {}) => {
  try {
    const sql = await database.sql();
    let query = sql`
      SELECT 
        i.*,
        p.product_name AS ref_product_name,
        p.product_category AS ref_product_category,
        p.product_image AS ref_product_image,
        c.category_name,
        w.warehouse_name
      FROM inventory_items i
      LEFT JOIN products p ON p.product_id = i.product_id
      LEFT JOIN categories c ON i.category_id = c.category_id
      LEFT JOIN warehouses w ON i.warehouse_id = w.warehouse_id
    `;
    
    let conditions = [];
    let params = [];
    
    if (filters.search) {
      conditions.push(`(i.product_name ILIKE $${params.length + 1} OR i.item_code ILIKE $${params.length + 1})`);
      params.push(`%${filters.search}%`);
    }
    
    if (filters.category) {
      conditions.push(`i.category_id = $${params.length + 1}`);
      params.push(filters.category);
    }
    
    if (filters.status) {
      conditions.push(`i.status = $${params.length + 1}`);
      params.push(filters.status);
    }
    
    if (filters.warehouse) {
      conditions.push(`i.warehouse_id = $${params.length + 1}`);
      params.push(filters.warehouse);
    }
    
    if (conditions.length > 0) {
      // Build filtered query using parameters array 'params'
      // Note: Neon tagged template cannot bind a dynamic number of placeholders easily,
      // so fall back to a single string with $1..$n placeholders executed via connection
      const connection = await database.sql();
      const where = `WHERE ${conditions.join(' AND ')}`;
      const text = `
        SELECT i.*, p.product_name AS ref_product_name, p.product_category AS ref_product_category,
               p.product_image AS ref_product_image, c.category_name, w.warehouse_name
        FROM inventory_items i
        LEFT JOIN products p ON p.product_id = i.product_id
        LEFT JOIN categories c ON i.category_id = c.category_id
        LEFT JOIN warehouses w ON i.warehouse_id = w.warehouse_id
        ${where}
        ORDER BY i.updated_at DESC`;
      query = connection.bind ? connection(text, params) : connection(text, params);
    } else {
      query = sql`
        SELECT 
          i.*,
          p.product_name AS ref_product_name,
          p.product_category AS ref_product_category,
          p.product_image AS ref_product_image,
          c.category_name,
          w.warehouse_name
        FROM inventory_items i
        LEFT JOIN products p ON p.product_id = i.product_id
        LEFT JOIN categories c ON i.category_id = c.category_id
        LEFT JOIN warehouses w ON i.warehouse_id = w.warehouse_id
        ORDER BY i.updated_at DESC
      `;
    }
    
    const result = await query;
    return result;
  } catch (err) {
    console.error('Error fetching inventory items:', err);
    throw err;
  }
};

// Get inventory item by ID
const getInventoryItemById = async (id) => {
  try {
    const sql = await database.sql();
    const result = await sql`
      SELECT 
        i.*,
        c.category_name,
        w.warehouse_name
      FROM inventory_items i
      LEFT JOIN categories c ON i.category_id = c.category_id
      LEFT JOIN warehouses w ON i.warehouse_id = w.warehouse_id
      WHERE i.id = ${id}
    `;
    
    return result[0] || null;
  } catch (err) {
    console.error('Error fetching inventory item:', err);
    throw err;
  }
};

// Create new inventory item
const createInventoryItem = async (itemData) => {
  try {
    const sql = await database.sql();
    const {
      item_code,
      product_id,
      product_pricing_id,
      product_name,
      unit_of_measure,
      buy_price,
      sell_price,
      location,
      category_id,
      status,
      warehouse_id,
      total_quantity
    } = itemData;
    
    const computedStatus = total_quantity > 0 ? 'active' : 'out of stock';
    
    const result = await sql`
      INSERT INTO inventory_items (
        item_code, product_id, product_pricing_id, product_name, unit_of_measure, buy_price, sell_price,
        location, category_id, status, warehouse_id, total_quantity, updated_at
      ) VALUES (
        ${item_code}, ${product_id || null}, ${product_pricing_id || null}, ${product_name}, ${unit_of_measure}, ${buy_price}, ${sell_price},
        ${location}, ${category_id}, ${computedStatus}, ${warehouse_id}, ${total_quantity}, CURRENT_TIMESTAMP
      )
      RETURNING *
    `;
    
    return result[0];
  } catch (err) {
    console.error('Error creating inventory item:', err);
    throw err;
  }
};

// Update inventory item
const updateInventoryItem = async (id, itemData) => {
  try {
    const sql = await database.sql();
    const {
      item_code,
      product_id,
      product_pricing_id,
      product_name,
      unit_of_measure,
      buy_price,
      sell_price,
      location,
      category_id,
      status,
      warehouse_id,
      total_quantity
    } = itemData;
    
    const computedStatus = total_quantity > 0 ? 'active' : 'out of stock';
    
    const result = await sql`
      UPDATE inventory_items SET
        item_code = ${item_code},
        product_id = ${product_id || null},
        product_pricing_id = ${product_pricing_id || null},
        product_name = ${product_name},
        unit_of_measure = ${unit_of_measure},
        buy_price = ${buy_price},
        sell_price = ${sell_price},
        location = ${location},
        category_id = ${category_id},
        status = ${computedStatus},
        warehouse_id = ${warehouse_id},
        total_quantity = ${total_quantity},
        updated_at = CURRENT_TIMESTAMP
      WHERE id = ${id}
      RETURNING *
    `;
    
    return result[0];
  } catch (err) {
    console.error('Error updating inventory item:', err);
    throw err;
  }
};

// Delete inventory item
const deleteInventoryItem = async (id) => {
  try {
    const sql = await database.sql();
    const result = await sql`
      DELETE FROM inventory_items
      WHERE id = ${id}
      RETURNING *
    `;
    
    return result[0];
  } catch (err) {
    console.error('Error deleting inventory item:', err);
    throw err;
  }
};

// Delete multiple inventory items
const deleteMultipleInventoryItems = async (ids) => {
  try {
    const sql = await database.sql();
    const result = await sql`
      DELETE FROM inventory_items
      WHERE id = ANY(${ids})
      RETURNING *
    `;
    
    return result;
  } catch (err) {
    console.error('Error deleting multiple inventory items:', err);
    throw err;
  }
};

// Get all categories
const getAllCategories = async () => {
  try {
    const sql = await database.sql();
    const result = await sql`
      SELECT category_id, category_name 
      FROM categories 
      ORDER BY category_id
    `;
    return result;
  } catch (err) {
    console.error('Error fetching categories:', err);
    throw err;
  }
};

// Get all warehouses
const getAllWarehouses = async () => {
  try {
    const sql = await database.sql();
    const result = await sql`
      SELECT * FROM warehouses
      ORDER BY warehouse_name
    `;
    
    return result;
  } catch (err) {
    console.error('Error fetching warehouses:', err);
    throw err;
  }
};

// Get inventory statistics
const getInventoryStats = async () => {
  try {
    const sql = await database.sql();
    const totalItems = await sql`SELECT COUNT(*) as count FROM inventory_items`;
    const activeItems = await sql`SELECT COUNT(*) as count FROM inventory_items WHERE status = 'active'`;
    const lowStockItems = await sql`SELECT COUNT(*) as count FROM inventory_items WHERE total_quantity < 10`;
    const totalValue = await sql`SELECT COALESCE(SUM(buy_price * total_quantity), 0) as total FROM inventory_items`;
    
    return {
      totalItems: parseInt(totalItems[0].count),
      activeItems: parseInt(activeItems[0].count),
      lowStockItems: parseInt(lowStockItems[0].count),
      totalValue: parseFloat(totalValue[0].total)
    };
  } catch (err) {
    console.error('Error fetching inventory statistics:', err);
    throw err;
  }
};

// Update item quantity
const updateItemQuantity = async (id, newQuantity, operation = 'set') => {
  try {
    const sql = await database.sql();
    let query;
    
    if (operation === 'add') {
      query = sql`
        UPDATE inventory_items 
        SET total_quantity = total_quantity + ${newQuantity}, updated_at = CURRENT_TIMESTAMP
        WHERE id = ${id}
        RETURNING *
      `;
    } else if (operation === 'subtract') {
      query = sql`
        UPDATE inventory_items 
        SET total_quantity = GREATEST(total_quantity - ${newQuantity}, 0), updated_at = CURRENT_TIMESTAMP
        WHERE id = ${id}
        RETURNING *
      `;
    } else {
      query = sql`
        UPDATE inventory_items 
        SET total_quantity = ${newQuantity}, updated_at = CURRENT_TIMESTAMP
        WHERE id = ${id}
        RETURNING *
      `;
    }
    
    const result = await query;
    return result[0];
  } catch (err) {
    console.error('Error updating item quantity:', err);
    throw err;
  }
};

// Get all material shipments with optional filters
const getAllMaterialShipments = async (filters = {}) => {
  try {
    const sql = await database.sql();
    let query = sql`
      SELECT * FROM material_shipments
    `;
    
    let conditions = [];
    let params = [];
    
    if (filters.search) {
      conditions.push(`(material_name ILIKE $${params.length + 1} OR shipment_id ILIKE $${params.length + 1} OR source ILIKE $${params.length + 1})`);
      params.push(`%${filters.search}%`);
    }
    
    if (filters.status) {
      conditions.push(`status = $${params.length + 1}`);
      params.push(filters.status);
    }
    
    if (filters.type) {
      conditions.push(`shipment_type = $${params.length + 1}`);
      params.push(filters.type);
    }
    
    if (filters.date) {
      conditions.push(`date_shipped = $${params.length + 1}`);
      params.push(filters.date);
    }
    
    if (conditions.length > 0) {
      const whereClause = ` WHERE ${conditions.join(' AND ')}`;
      query = sql`
        SELECT * FROM material_shipments
        ${sql.unsafe(whereClause)}
        ORDER BY created_at DESC
      `;
    } else {
      query = sql`
        SELECT * FROM material_shipments
        ORDER BY created_at DESC
      `;
    }
    
    const result = await query;
    return result;
  } catch (err) {
    console.error('Error fetching material shipments:', err);
    throw err;
  }
};

// Get material shipment by ID
const getMaterialShipmentById = async (id) => {
  try {
    const sql = await database.sql();
    const result = await sql`
      SELECT * FROM material_shipments
      WHERE id = ${id}
    `;
    
    return result[0] || null;
  } catch (err) {
    console.error('Error fetching material shipment:', err);
    throw err;
  }
};

// Create new material shipment
const createMaterialShipment = async (shipmentData) => {
  try {
    const sql = await database.sql();
    const {
      shipment_id,
      bom_id,      
      category_id,
      material_name,
      quantity,
      unit,
      shipment_type,
      source,
      destination,
      status,
      date_shipped,
      estimated_delivery,
      received_date,
      handled_by,
      notes
    } = shipmentData;
    
    const result = await sql`
      INSERT INTO material_shipments (
        shipment_id, bom_id, category_id, material_name, quantity, unit, 
        shipment_type, source, destination, status, date_shipped, 
        estimated_delivery, received_date, handled_by, notes, updated_at
      ) VALUES (
        ${shipment_id}, ${bom_id}, ${category_id}, ${material_name}, 
        ${quantity}, ${unit}, ${shipment_type}, ${source}, ${destination}, 
        ${status}, ${date_shipped}, ${estimated_delivery}, ${received_date}, 
        ${handled_by}, ${notes}, CURRENT_TIMESTAMP
      )
      RETURNING *
    `;
    
    return result[0];
  } catch (err) {
    console.error('Error creating material shipment:', err);
    throw err;
  }
};

// Update material shipment
const updateMaterialShipment = async (id, shipmentData) => {
  try {
    const sql = await database.sql();
    const {
      shipment_id,
      bom_id,        // Changed from material_id
      category_id,
      material_name,
      quantity,
      unit,
      shipment_type,
      source,
      destination,
      status,
      date_shipped,
      estimated_delivery,
      received_date,
      handled_by,
      notes
    } = shipmentData;
    
    const result = await sql`
      UPDATE material_shipments SET
        shipment_id = ${shipment_id},
        bom_id = ${bom_id},        /* Changed from material_id */
        category_id = ${category_id},
        material_name = ${material_name},
        quantity = ${quantity},
        unit = ${unit},
        shipment_type = ${shipment_type},
        source = ${source},
        destination = ${destination},
        status = ${status},
        date_shipped = ${date_shipped},
        estimated_delivery = ${estimated_delivery},
        received_date = ${received_date},
        handled_by = ${handled_by},
        notes = ${notes},
        updated_at = CURRENT_TIMESTAMP
      WHERE id = ${id}
      RETURNING *
    `;
    
    return result[0];
  } catch (err) {
    console.error('Error updating material shipment:', err);
    throw err;
  }
};

// Delete material shipment
const deleteMaterialShipment = async (id) => {
  try {
    const sql = await database.sql();
    const result = await sql`
      DELETE FROM material_shipments
      WHERE id = ${id}
      RETURNING *
    `;
    
    return result[0];
  } catch (err) {
    console.error('Error deleting material shipment:', err);
    throw err;
  }
};

// Get material shipment statistics
const getMaterialShipmentStats = async () => {
  try {
    const sql = await database.sql();
    const totalShipments = await sql`SELECT COUNT(*) as count FROM material_shipments`;
    const deliveredShipments = await sql`SELECT COUNT(*) as count FROM material_shipments WHERE status = 'delivered'`;
    const pendingShipments = await sql`SELECT COUNT(*) as count FROM material_shipments WHERE status = 'pending'`;
    const shippedShipments = await sql`SELECT COUNT(*) as count FROM material_shipments WHERE status = 'shipped'`;
    const inboundShipments = await sql`SELECT COUNT(*) as count FROM material_shipments WHERE shipment_type = 'inbound'`;
    const outboundShipments = await sql`SELECT COUNT(*) as count FROM material_shipments WHERE shipment_type = 'outbound'`;
    
    return {
      totalShipments: parseInt(totalShipments[0].count),
      deliveredShipments: parseInt(deliveredShipments[0].count),
      pendingShipments: parseInt(pendingShipments[0].count),
      shippedShipments: parseInt(shippedShipments[0].count),
      inboundShipments: parseInt(inboundShipments[0].count),
      outboundShipments: parseInt(outboundShipments[0].count)
    };
  } catch (err) {
    console.error('Error fetching material shipment statistics:', err);
    throw err;
  }
};

// Update shipment status
const updateShipmentStatus = async (id, status, receivedDate = null) => {
  try {
    const sql = await database.sql();
    let query;
    
    if (receivedDate) {
      query = sql`
        UPDATE material_shipments 
        SET status = ${status}, received_date = ${receivedDate}, updated_at = CURRENT_TIMESTAMP
        WHERE id = ${id}
        RETURNING *
      `;
    } else {
      query = sql`
        UPDATE material_shipments 
        SET status = ${status}, updated_at = CURRENT_TIMESTAMP
        WHERE id = ${id}
        RETURNING *
      `;
    }
    
    const result = await query;
    return result[0];
  } catch (err) {
    console.error('Error updating shipment status:', err);
    throw err;
  }
};

// Get shipments by inventory item
const getShipmentsByCategoryId = async (categoryId) => {
  try {
    const sql = await database.sql();
    const result = await sql`
      SELECT * FROM material_shipments
      WHERE category_id = ${categoryId}
      ORDER BY created_at DESC
    `;
    
    return result;
  } catch (err) {
    console.error('Error fetching shipments by category ID:', err);
    throw err;
  }
};

// Get all order shipments with optional filters
const getAllOrderShipments = async (filters = {}) => {
  try {
    const sql = await database.sql();
    let base = sql`
      SELECT * FROM order_shipments
    `;

    const conditions = [];
    const params = [];

    if (filters.search) {
      conditions.push(`(order_id ILIKE $${params.length + 1} OR customer_name ILIKE $${params.length + 1} OR product_name ILIKE $${params.length + 1} OR item_code ILIKE $${params.length + 1})`);
      params.push(`%${filters.search}%`);
    }
    if (filters.status) {
      conditions.push(`status = $${params.length + 1}`);
      params.push(filters.status);
    }
    if (filters.priority) {
      conditions.push(`priority = $${params.length + 1}`);
      params.push(filters.priority);
    }
    if (filters.date) {
      conditions.push(`order_date = $${params.length + 1}`);
      params.push(filters.date);
    }

    if (conditions.length > 0) {
      const whereClause = ` WHERE ${conditions.join(' AND ')}`;
      base = sql`
        SELECT * FROM order_shipments
        ${sql.unsafe(whereClause)}
        ORDER BY created_at DESC
      `;
    } else {
      base = sql`
        SELECT * FROM order_shipments
        ORDER BY created_at DESC
      `;
    }

    const result = await base;
    return result;
  } catch (err) {
    console.error('Error fetching order shipments:', err);
    throw err;
  }
};

// Get order shipment by ID
const getOrderShipmentById = async (id) => {
  try {
    const sql = await database.sql();
    const result = await sql`
      SELECT * FROM order_shipments WHERE id = ${id}
    `;
    return result[0] || null;
  } catch (err) {
    console.error('Error fetching order shipment:', err);
    throw err;
  }
};

// Create new order shipment
const createOrderShipment = async (orderData) => {
  try {
    const sql = await database.sql();
    const {
      order_id,
      customer_name,
      item_code,
      product_name,
      quantity,
      total_value,
      priority,
      status,
      order_date,
      ship_date,
      delivery_date,
      tracking_number,
      notes
    } = orderData;

    const result = await sql`
      INSERT INTO order_shipments (
        order_id, customer_name, item_code, product_name, quantity, total_value,
        priority, status, order_date, ship_date, delivery_date, tracking_number, notes, updated_at
      ) VALUES (
        ${order_id}, ${customer_name}, ${item_code}, ${product_name}, ${quantity}, ${total_value},
        ${priority || 'medium'}, ${status || 'processing'}, ${order_date}, ${ship_date}, ${delivery_date}, ${tracking_number}, ${notes}, CURRENT_TIMESTAMP
      )
      RETURNING *
    `;

    return result[0];
  } catch (err) {
    console.error('Error creating order shipment:', err);
    throw err;
  }
};

// Update order shipment
const updateOrderShipment = async (id, orderData) => {
  try {
    const sql = await database.sql();
    const {
      order_id,
      customer_name,
      item_code,
      product_name,
      quantity,
      total_value,
      priority,
      status,
      order_date,
      ship_date,
      delivery_date,
      tracking_number,
      notes
    } = orderData;

    const result = await sql`
      UPDATE order_shipments SET
        order_id = ${order_id},
        customer_name = ${customer_name},
        item_code = ${item_code},
        product_name = ${product_name},
        quantity = ${quantity},
        total_value = ${total_value},
        priority = ${priority},
        status = ${status},
        order_date = ${order_date},
        ship_date = ${ship_date},
        delivery_date = ${delivery_date},
        tracking_number = ${tracking_number},
        notes = ${notes},
        updated_at = CURRENT_TIMESTAMP
      WHERE id = ${id}
      RETURNING *
    `;

    return result[0];
  } catch (err) {
    console.error('Error updating order shipment:', err);
    throw err;
  }
};

// Delete order shipment
const deleteOrderShipment = async (id) => {
  try {
    const sql = await database.sql();
    const result = await sql`
      DELETE FROM order_shipments WHERE id = ${id}
      RETURNING *
    `;
    return result[0];
  } catch (err) {
    console.error('Error deleting order shipment:', err);
    throw err;
  }
};

// Get order shipment statistics
const getOrderShipmentStats = async () => {
  try {
    const sql = await database.sql();
    const total = await sql`SELECT COUNT(*) as count FROM order_shipments`;
    const delivered = await sql`SELECT COUNT(*) as count FROM order_shipments WHERE status = 'delivered'`;
    const shipped = await sql`SELECT COUNT(*) as count FROM order_shipments WHERE status = 'shipped'`;
    const processing = await sql`SELECT COUNT(*) as count FROM order_shipments WHERE status = 'processing'`;

    return {
      totalOrders: parseInt(total[0].count),
      deliveredOrders: parseInt(delivered[0].count),
      shippedOrders: parseInt(shipped[0].count),
      processingOrders: parseInt(processing[0].count)
    };
  } catch (err) {
    console.error('Error fetching order shipment statistics:', err);
    throw err;
  }
};

// Update order shipment status
const updateOrderShipmentStatus = async (id, status, options = {}) => {
  try {
    const sql = await database.sql();
    const setShipDate = options.setShipDate ? options.setShipDate : null;
    const setDeliveryDate = options.setDeliveryDate ? options.setDeliveryDate : null;

    const result = await sql`
      UPDATE order_shipments
      SET status = ${status},
          ship_date = COALESCE(${setShipDate}, ship_date),
          delivery_date = COALESCE(${setDeliveryDate}, delivery_date),
          updated_at = CURRENT_TIMESTAMP
      WHERE id = ${id}
      RETURNING *
    `;

    return result[0];
  } catch (err) {
    console.error('Error updating order status:', err);
    throw err;
  }
};

// Get all products with latest pricing and computed stock for inventory view
const getAllProductsInventory = async () => {
  try {
    const sql = await database.sql();
    const result = await sql`
      WITH latest_pricing AS (
        SELECT DISTINCT ON (product_id)
          product_id,
          price,
          discount_rate,
          effective_date
        FROM product_pricing
        ORDER BY product_id, effective_date DESC
      ),
      stock_by_product AS (
        SELECT product_id, SUM(total_quantity) AS total_quantity
        FROM inventory_items
        GROUP BY product_id
      )
      SELECT 
        p.product_id AS id,
        p.product_id AS product_id,
        p.product_id::text AS item_code,
        p.product_name,
        'PCS'::varchar(10) AS unit_of_measure,
        NULL::DECIMAL(10,2) AS buy_price,
        COALESCE(lp.price * (1 - COALESCE(lp.discount_rate, 0)), lp.price) AS sell_price,
        NULL::varchar(255) AS location,
        p.product_category AS product_category,
        CASE WHEN COALESCE(s.total_quantity, 0) > 0 THEN 'active' ELSE 'out of stock' END AS status,
        NULL::varchar(50) AS warehouse_id,
        COALESCE(s.total_quantity, 0) AS total_quantity,
        COALESCE(lp.effective_date, CURRENT_DATE) AS updated_at,
        p.product_image
      FROM products p
      LEFT JOIN latest_pricing lp ON lp.product_id = p.product_id
      LEFT JOIN stock_by_product s ON s.product_id = p.product_id
      ORDER BY p.product_id
    `;

    return result.map(r => ({
      ...r,
      category_id: r.product_category
    }));
  } catch (err) {
    console.error('Error fetching products inventory:', err);
    throw err;
  }
};

// Fetch one product plus its latest pricing
const getProductWithLatestPricing = async (productId) => {
  try {
    const sql = await database.sql();
    const rows = await sql`
      WITH latest_pricing AS (
        SELECT DISTINCT ON (product_id)
          id,
          product_id,
          price,
          discount_rate,
          effective_date
        FROM product_pricing
        WHERE product_id = ${productId}
        ORDER BY product_id, effective_date DESC
      )
      SELECT p.product_id, p.product_name, p.product_category, p.product_image,
             lp.id AS pricing_id,
             lp.price,
             lp.discount_rate,
             lp.effective_date,
             COALESCE(lp.price * (1 - COALESCE(lp.discount_rate, 0)), lp.price) AS net_price
      FROM products p
      LEFT JOIN latest_pricing lp ON lp.product_id = p.product_id
      WHERE p.product_id = ${productId}
      LIMIT 1
    `;
    return rows[0] || null;
  } catch (err) {
    console.error('Error fetching product with pricing:', err);
    throw err;
  }
};


module.exports = {
  initializeInventoryTable,
  initializeMaterialShipmentsTable,
  initializeOrderShipmentsTable,
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
  getAllMaterialShipments,
  getMaterialShipmentById,
  createMaterialShipment,
  updateMaterialShipment,
  deleteMaterialShipment,
  getMaterialShipmentStats,
  updateShipmentStatus,
  getShipmentsByCategoryId,
  getAllOrderShipments,
  getOrderShipmentById,
  createOrderShipment,
  updateOrderShipment,
  deleteOrderShipment,
  getOrderShipmentStats,
  updateOrderShipmentStatus,
  getAllProductsInventory,
  getProductWithLatestPricing
};
