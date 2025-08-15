const database = require('./database');

// Initialize inventory table
const initializeInventoryTable = async () => {
  try {
    const sql = await database.sql();
    await sql`
      CREATE TABLE IF NOT EXISTS inventory_items (
        id SERIAL PRIMARY KEY,
        item_code VARCHAR(50) UNIQUE NOT NULL,
        product_id INTEGER NOT NULL,
        unit_of_measure VARCHAR(10) NOT NULL,
        category_id VARCHAR(50),
        status VARCHAR(20),
        warehouse_id VARCHAR(50),
        total_quantity INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_inventory_product FOREIGN KEY (product_id) REFERENCES products(product_id)
      )
    `;
    
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
    
    // Ensure dependent tables exist (products, product_pricing)
    await sql`
      CREATE TABLE IF NOT EXISTS products (
        product_id SERIAL PRIMARY KEY,
        product_name VARCHAR(255) NOT NULL,
        product_description TEXT,
        product_category VARCHAR(100),
        product_image TEXT
      )
    `;
    await sql`
      CREATE TABLE IF NOT EXISTS product_pricing (
        id SERIAL PRIMARY KEY,
        product_id INTEGER REFERENCES products(product_id),
        price NUMERIC(10,2) NOT NULL,
        discount_rate NUMERIC(10,2) DEFAULT 0,
        effective_date DATE DEFAULT CURRENT_DATE,
        updated_by INTEGER
      )
    `;
    
    // Migrate legacy inventory_items columns if the table already existed
    await sql`ALTER TABLE inventory_items ADD COLUMN IF NOT EXISTS product_id INTEGER`;
    await sql`ALTER TABLE inventory_items DROP COLUMN IF EXISTS product_name`;
    await sql`ALTER TABLE inventory_items DROP COLUMN IF EXISTS buy_price`;
    await sql`ALTER TABLE inventory_items DROP COLUMN IF EXISTS sell_price`;
    await sql`ALTER TABLE inventory_items DROP COLUMN IF EXISTS location`;
    // Add FK if missing (guarded)
    await sql`
      DO $$ BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM information_schema.table_constraints
          WHERE constraint_name = 'fk_inventory_product'
            AND table_name = 'inventory_items'
        ) THEN
          ALTER TABLE inventory_items
          ADD CONSTRAINT fk_inventory_product FOREIGN KEY (product_id) REFERENCES products(product_id);
        END IF;
      END $$;
    `;
    
    // Material shipments removed
    
    
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

// Initialize order shipments table
const initializeOrderShipmentsTable = async () => {
  try {
    const sql = await database.sql();
    // Ensure core tables exist
    await sql`
      CREATE TABLE IF NOT EXISTS businesses (
        id SERIAL PRIMARY KEY
      )
    `;
    await sql`
      CREATE TABLE IF NOT EXISTS customers (
        customer_id SERIAL PRIMARY KEY,
        business_id INT REFERENCES businesses(id) ON DELETE CASCADE,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        email VARCHAR(255) UNIQUE,
        phone VARCHAR(50),
        created_at TIMESTAMP DEFAULT NOW()
      )
    `;

    // Create production_planning table and trigger to populate from sales_orders
    await sql`
      CREATE TABLE IF NOT EXISTS production_planning (
        plan_id SERIAL PRIMARY KEY,
        order_id INT UNIQUE,
        customer_id INT,
        product_id INT UNIQUE,
        product_name VARCHAR(255),
        work_order_id SERIAL UNIQUE,
        planned_date DATE,
        status VARCHAR(50),
        quantity INT
      )
    `;

    const createPPTriggerFunction = `
      CREATE OR REPLACE FUNCTION insert_pp_after_orders_insert()
      RETURNS TRIGGER AS $$
      BEGIN
      IF NEW.payment_status = 'paid' THEN
      INSERT INTO production_planning (
      order_id,
      customer_id,
      product_id,
      product_name,
      planned_date,
      status,
      quantity
      )
      VALUES (
      NEW.order_id,
      NEW.customer_id,
      NEW.product_id,
      (SELECT product_name FROM products WHERE product_id = NEW.product_id),
      NEW.order_date,
      'processing',
      NEW.quantity
      );
      END IF;


      RETURN NEW;

      END;
      $$ LANGUAGE plpgsql;`;
    await sql(createPPTriggerFunction);

    // Add customer_id column to production_planning if it doesn't exist
    await sql`ALTER TABLE production_planning ADD COLUMN IF NOT EXISTS customer_id INT`;
    
    // Handle duplicate product_id values before adding UNIQUE constraint
    await sql`
      DO $$ 
      BEGIN
        -- First, check if there are duplicates
        IF EXISTS (
          SELECT product_id FROM production_planning 
          WHERE product_id IS NOT NULL 
          GROUP BY product_id 
          HAVING COUNT(*) > 1
        ) THEN
          -- Remove duplicates, keeping only the latest record for each product_id
          DELETE FROM production_planning 
          WHERE plan_id NOT IN (
            SELECT MAX(plan_id) 
            FROM production_planning 
            WHERE product_id IS NOT NULL 
            GROUP BY product_id
          ) AND product_id IS NOT NULL;
        END IF;
        
        -- Now add the UNIQUE constraint if it doesn't exist
        IF NOT EXISTS (
          SELECT 1 FROM information_schema.table_constraints 
          WHERE table_name = 'production_planning' 
          AND constraint_name = 'production_planning_product_id_key'
        ) THEN
          ALTER TABLE production_planning ADD CONSTRAINT production_planning_product_id_key UNIQUE (product_id);
        END IF;
      END $$;
    `;

    await sql(`
      DO $$
      BEGIN
          IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'sales_orders') THEN
              -- Drop existing trigger if it exists
              DROP TRIGGER IF EXISTS trg_insert_pp ON sales_orders;
              -- Create the new trigger
              CREATE TRIGGER trg_insert_pp
              AFTER INSERT ON sales_orders
              FOR EACH ROW
              EXECUTE FUNCTION insert_pp_after_orders_insert();
          END IF;
      END $$;
    `);

    // Removed sales_orders linkage; relying on production_planning
    await sql`
      CREATE TABLE IF NOT EXISTS order_shipments (
        id SERIAL PRIMARY KEY,
        order_id VARCHAR(50) NOT NULL,
        item_code VARCHAR(50),
        quantity INTEGER,
        total_value NUMERIC(12,2),
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
    await sql`ALTER TABLE order_shipments ADD COLUMN IF NOT EXISTS product_id INTEGER`;
    await sql`ALTER TABLE order_shipments ADD COLUMN IF NOT EXISTS product_name VARCHAR(255)`;

    // Try to align order_id with production_planning if it exists; else fallback to sales_orders
    const ppCol = await sql`
      SELECT data_type, udt_name, character_maximum_length
      FROM information_schema.columns
      WHERE table_name = 'production_planning' AND column_name = 'order_id'
      LIMIT 1
    `;
    let targetType = null;
    let castType = 'text';
    if (ppCol && ppCol.length) {
      const ppType = String(ppCol[0].data_type || '').toLowerCase();
      const ppUdt = String(ppCol[0].udt_name || '').toLowerCase();
      if (ppType.includes('integer') || ppUdt === 'int4') {
        targetType = 'INTEGER';
        castType = 'integer';
      } else if (ppUdt === 'int8' || ppType.includes('bigint')) {
        targetType = 'BIGINT';
        castType = 'bigint';
      }
    }
    if (!targetType) {
      const soCol = await sql`
        SELECT data_type, udt_name, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = 'sales_orders' AND column_name = 'order_id'
        LIMIT 1
      `;
      if (soCol && soCol.length) {
        const soType = String(soCol[0].data_type || '').toLowerCase();
        const soUdt = String(soCol[0].udt_name || '').toLowerCase();
        const soLen = soCol[0].character_maximum_length || 50;
        targetType = 'VARCHAR(' + soLen + ')';
        castType = 'text';
        if (soType.includes('integer') || soUdt === 'int4') {
          targetType = 'INTEGER';
          castType = 'integer';
        } else if (soUdt === 'int8' || soType.includes('bigint')) {
          targetType = 'BIGINT';
          castType = 'bigint';
        }
      }
    }

    if (targetType) {
      const osCol = await sql`
        SELECT data_type, udt_name, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = 'order_shipments' AND column_name = 'order_id'
        LIMIT 1
      `;
      let needAlter = true;
      if (osCol && osCol.length) {
        const osType = String(osCol[0].data_type || '').toLowerCase();
        const osUdt = String(osCol[0].udt_name || '').toLowerCase();
        const osLen = osCol[0].character_maximum_length;
        if ((String(targetType).startsWith('VARCHAR') && osType.includes('character')) ||
            (targetType === 'INTEGER' && (osType.includes('integer') || osUdt === 'int4')) ||
            (targetType === 'BIGINT' && (osType.includes('bigint') || osUdt === 'int8'))) {
          needAlter = false;
        }
      }
      if (needAlter) {
        const alter = `ALTER TABLE order_shipments ALTER COLUMN order_id TYPE ${targetType} USING order_id::${castType}`;
        await sql(alter);
      }
    }
 
    // Add FK to production_planning(order_id) only if order_id is unique/PK
    const ppInfo = await sql`
      SELECT tc.constraint_type
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name AND tc.table_name = kcu.table_name
      WHERE tc.table_name = 'production_planning' AND kcu.column_name = 'order_id'
    `;
    const hasUniqueOnPP = (ppInfo || []).some(r => ['PRIMARY KEY','UNIQUE'].includes(String(r.constraint_type || '').toUpperCase()));

    // Drop any legacy FK to sales_orders
    await sql`DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE table_name = 'order_shipments' AND constraint_name = 'fk_shipments_sales_order') THEN ALTER TABLE order_shipments DROP CONSTRAINT fk_shipments_sales_order; END IF; END $$;`;

    // Drop any existing FK to production_planning to avoid conflicts during migration
    await sql`DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE table_name = 'order_shipments' AND constraint_name = 'fk_shipments_planning_order') THEN ALTER TABLE order_shipments DROP CONSTRAINT fk_shipments_planning_order; END IF; END $$;`;

    // Clean up orphaned order_shipments records that don't have corresponding production_planning entries
    await sql`
      DELETE FROM order_shipments 
      WHERE order_id::text NOT IN (
        SELECT order_id::text FROM production_planning WHERE order_id IS NOT NULL
      )
      AND order_id IS NOT NULL
    `;

    if (hasUniqueOnPP) {
      await sql`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE table_name = 'order_shipments' AND constraint_name = 'fk_shipments_planning_order') THEN ALTER TABLE order_shipments ADD CONSTRAINT fk_shipments_planning_order FOREIGN KEY (order_id) REFERENCES production_planning(order_id) ON DELETE CASCADE; END IF; END $$;`;
    }

    // Align and add customer_id on order_shipments based on sales_orders.customer_id if available
    let customerIdTargetType = 'VARCHAR(50)';
    let customerIdCastType = 'text';
    const soCustCol = await sql`
      SELECT data_type, udt_name, character_maximum_length
      FROM information_schema.columns
      WHERE table_name = 'sales_orders' AND column_name = 'customer_id'
      LIMIT 1
    `;
    if (soCustCol && soCustCol.length) {
      const soType = String(soCustCol[0].data_type || '').toLowerCase();
      const soUdt = String(soCustCol[0].udt_name || '').toLowerCase();
      const soLen = soCustCol[0].character_maximum_length || 50;
      customerIdTargetType = 'VARCHAR(' + soLen + ')';
      if (soType.includes('integer') || soUdt === 'int4') {
        customerIdTargetType = 'INTEGER';
        customerIdCastType = 'integer';
      } else if (soUdt === 'int8' || soType.includes('bigint')) {
        customerIdTargetType = 'BIGINT';
        customerIdCastType = 'bigint';
      }
    }
    // Ensure column exists
    await sql(`ALTER TABLE order_shipments ADD COLUMN IF NOT EXISTS customer_id ${customerIdTargetType}`);
    // Align type if needed
    const osCustCol = await sql`
      SELECT data_type, udt_name, character_maximum_length
      FROM information_schema.columns
      WHERE table_name = 'order_shipments' AND column_name = 'customer_id'
      LIMIT 1
    `;
    if (osCustCol && osCustCol.length) {
      const osType = String(osCustCol[0].data_type || '').toLowerCase();
      const osUdt = String(osCustCol[0].udt_name || '').toLowerCase();
      const osLen = osCustCol[0].character_maximum_length;
      const needAlterCust = (
        (String(customerIdTargetType).startsWith('VARCHAR') && (!osType.includes('character'))) ||
        (customerIdTargetType === 'INTEGER' && !(osType.includes('integer') || osUdt === 'int4')) ||
        (customerIdTargetType === 'BIGINT' && !(osType.includes('bigint') || osUdt === 'int8'))
      );
      if (needAlterCust) {
        const alterCust = `ALTER TABLE order_shipments ALTER COLUMN customer_id TYPE ${customerIdTargetType} USING customer_id::${customerIdCastType}`;
        await sql(alterCust);
      }
    }

    // Conditionally add FK to customers(customer_id)
    await sql`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE table_name = 'order_shipments' AND constraint_name = 'fk_shipments_customer') THEN ALTER TABLE order_shipments ADD CONSTRAINT fk_shipments_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE SET NULL; END IF; END $$;`;
    console.log('✅ Order shipments table created/verified');
  } catch (err) {
    console.error('❌ Error creating order shipments table:', err);
    throw err;
  }
};

// Insert shipments for processed production plans not yet in shipments
const syncProcessedPlansIntoShipments = async () => {
  const sql = await database.sql();
  // Only run if production_planning table exists
  const t = await sql`SELECT to_regclass('public.production_planning') AS reg`;
  if (!t.length || !t[0].reg) return;
  const queryText = `
    INSERT INTO order_shipments (
      order_id, product_id, product_name, quantity,
      status, order_date, updated_at
    )
    SELECT
      pp.order_id,
      pp.product_id,
      pp.product_name,
      pp.quantity,
      'processed' AS status,
      pp.planned_date AS order_date,
      CURRENT_TIMESTAMP
    FROM production_planning pp
    WHERE pp.status = 'processed'
      AND NOT EXISTS (
        SELECT 1 FROM order_shipments os WHERE os.order_id::text = pp.order_id::text
      )`;
  await sql(queryText);
};

// Get all inventory items with optional filters
const getAllInventoryItems = async (filters = {}) => {
  try {
    const sql = await database.sql();

    // Base select with latest price via LATERAL join
    let queryText = `
      SELECT 
        i.id,
        i.item_code,
        i.product_id,
        p.product_name,
        p.product_description,
        p.product_category,
        p.product_image,
        pp.price,
        i.unit_of_measure,
        i.category_id,
        i.status,
        i.warehouse_id,
        i.total_quantity,
        i.created_at,
        i.updated_at,
        c.category_name,
        w.warehouse_name
      FROM inventory_items i
      LEFT JOIN categories c ON i.category_id = c.category_id
      LEFT JOIN warehouses w ON i.warehouse_id = w.warehouse_id
      LEFT JOIN products p ON p.product_id = i.product_id
      LEFT JOIN LATERAL (
        SELECT price
        FROM product_pricing ppx
        WHERE ppx.product_id = i.product_id
        ORDER BY effective_date DESC
        LIMIT 1
      ) pp ON true
    `;

    const params = [];
    const conds = [];

    if (filters && typeof filters.search === 'string' && filters.search.trim() !== '') {
      const term = `%${filters.search.trim()}%`;
      conds.push(`(p.product_name ILIKE $${params.length + 1} OR i.item_code ILIKE $${params.length + 2})`);
      params.push(term, term);
    }
    if (filters && filters.category) {
      conds.push(`i.category_id = $${params.length + 1}`);
      params.push(filters.category);
    }
    if (filters && filters.status) {
      conds.push(`i.status = $${params.length + 1}`);
      params.push(filters.status);
    }
    if (filters && filters.warehouse) {
      conds.push(`i.warehouse_id = $${params.length + 1}`);
      params.push(filters.warehouse);
    }

    if (conds.length) {
      queryText += ` WHERE ${conds.join(' AND ')}`;
    }

    queryText += ` ORDER BY i.updated_at DESC`;

    const result = await sql(queryText, params);
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
        i.id,
        i.item_code,
        i.product_id,
        p.product_name,
        p.product_description,
        p.product_category,
        p.product_image,
        pp.price,
        i.unit_of_measure,
        i.category_id,
        i.status,
        i.warehouse_id,
        i.total_quantity,
        i.created_at,
        i.updated_at,
        c.category_name,
        w.warehouse_name
      FROM inventory_items i
      LEFT JOIN categories c ON i.category_id = c.category_id
      LEFT JOIN warehouses w ON i.warehouse_id = w.warehouse_id
      LEFT JOIN products p ON p.product_id = i.product_id
      LEFT JOIN product_pricing pp ON pp.product_id = i.product_id
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
      unit_of_measure,
      category_id,
      status,
      warehouse_id,
      total_quantity
    } = itemData;
    
    const result = await sql`
      INSERT INTO inventory_items (
        item_code, product_id, unit_of_measure, category_id, status, warehouse_id, total_quantity, updated_at
      ) VALUES (
        ${item_code}, ${product_id}, ${unit_of_measure}, ${category_id}, ${status}, ${warehouse_id}, ${total_quantity}, CURRENT_TIMESTAMP
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
      unit_of_measure,
      category_id,
      status,
      warehouse_id,
      total_quantity
    } = itemData;
    
    const result = await sql`
      UPDATE inventory_items SET
        item_code = ${item_code},
        product_id = ${product_id},
        unit_of_measure = ${unit_of_measure},
        category_id = ${category_id},
        status = ${status},
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
    const totalValue = await sql`
      SELECT COALESCE(SUM(COALESCE(pp.price,0) * i.total_quantity), 0) as total
      FROM inventory_items i
      LEFT JOIN product_pricing pp ON pp.product_id = i.product_id
    `;
    
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

// Add: Stock overview with configurable low stock threshold
const getStockOverview = async (options = {}) => {
  const threshold = Number(options.threshold) || 50;
  try {
    const sql = await database.sql();
    if (!sql) {
      return {
        totalItems: 0,
        totalStockQuantity: 0,
        lowStockItems: 0,
        outOfStockItems: 0
      };
    }
    const [totalItems, totalStock, lowStock, outOfStock] = await Promise.all([
      sql`SELECT COUNT(*)::int AS count FROM inventory_items`,
      sql`SELECT COALESCE(SUM(total_quantity), 0)::bigint AS sum FROM inventory_items`,
      sql`SELECT COUNT(*)::int AS count FROM inventory_items WHERE total_quantity > 0 AND total_quantity < ${threshold}`,
      sql`SELECT COUNT(*)::int AS count FROM inventory_items WHERE status = 'out of stock' OR total_quantity = 0`
    ]);

    return {
      totalItems: Number(totalItems[0].count),
      totalStockQuantity: Number(totalStock[0].sum),
      lowStockItems: Number(lowStock[0].count),
      outOfStockItems: Number(outOfStock[0].count)
    };
  } catch (err) {
    console.error('Error fetching stock overview:', err);
    return {
      totalItems: 0,
      totalStockQuantity: 0,
      lowStockItems: 0,
      outOfStockItems: 0
    };
  }
};

// Add: Stock aggregated by product category (from products table)
const getStockByCategory = async () => {
  try {
    const sql = await database.sql();
    if (!sql) return [];
    const rows = await sql`
      SELECT 
        COALESCE(NULLIF(TRIM(p.product_category), ''), 'Uncategorized') AS category,
        SUM(i.total_quantity)::bigint AS total_quantity,
        COUNT(*)::int AS item_count
      FROM inventory_items i
      LEFT JOIN products p ON p.product_id = i.product_id
      GROUP BY category
      ORDER BY total_quantity DESC
    `;
    return rows;
  } catch (err) {
    console.error('Error fetching stock by category:', err);
    return [];
  }
};

// Add: Stock aggregated by warehouse
const getStockByWarehouse = async () => {
  try {
    const sql = await database.sql();
    if (!sql) return [];
    const rows = await sql`
      SELECT 
        i.warehouse_id,
        COALESCE(w.warehouse_name, i.warehouse_id) AS warehouse_name,
        SUM(i.total_quantity)::bigint AS total_quantity,
        COUNT(*)::int AS item_count
      FROM inventory_items i
      LEFT JOIN warehouses w ON w.warehouse_id = i.warehouse_id
      GROUP BY i.warehouse_id, w.warehouse_name
      ORDER BY total_quantity DESC
    `;
    return rows;
  } catch (err) {
    console.error('Error fetching stock by warehouse:', err);
    return [];
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
        SET total_quantity = GREATEST(total_quantity - ${newQuantity}, 0),
            status = CASE WHEN GREATEST(total_quantity - ${newQuantity}, 0) <= 0 THEN 'out of stock' ELSE status END,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ${id}
        RETURNING *
      `;
    } else {
      query = sql`
        UPDATE inventory_items 
        SET total_quantity = ${newQuantity},
            status = CASE WHEN ${newQuantity} <= 0 THEN 'out of stock' ELSE status END,
            updated_at = CURRENT_TIMESTAMP
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

// Material shipments removed

// Get all order shipments with optional filters
const getAllOrderShipments = async (filters = {}) => {
  try {
    const sql = await database.sql();
    // Ensure shipments reflect processed production plans
    await syncProcessedPlansIntoShipments();

    let queryText = `
      SELECT 
        os.*
      FROM order_shipments os
    `;

    const conditions = [];
    const params = [];

    if (filters.search) {
      const term = `%${filters.search}%`;
      conditions.push(`(os.order_id::text ILIKE $${params.length + 1} OR os.product_name ILIKE $${params.length + 1})`);
      params.push(term);
    }
    if (filters.status) {
      conditions.push(`os.status = $${params.length + 1}`);
      params.push(filters.status);
    }
    if (filters.priority) {
      // priority column no longer exists; ignore
    }
    if (filters.date) {
      conditions.push(`os.order_date = $${params.length + 1}`);
      params.push(filters.date);
    }

    if (conditions.length > 0) {
      queryText += ` WHERE ${conditions.join(' AND ')}`;
    }

    queryText += ` ORDER BY os.updated_at DESC`;

    const result = await sql(queryText, params);
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
      SELECT 
        os.*
      FROM order_shipments os
      WHERE os.id = ${id}
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
      customer_id,
      item_code,
      quantity,
      total_value,
      status,
      order_date,
      ship_date,
      delivery_date,
      tracking_number,
      notes
    } = orderData;

    const result = await sql`
      INSERT INTO order_shipments (
        order_id, customer_id, item_code, quantity, total_value,
        status, order_date, ship_date, delivery_date, tracking_number, notes, updated_at
      ) VALUES (
        ${order_id}, ${customer_id}, ${item_code}, ${quantity}, ${total_value},
        ${status || 'processing'}, ${order_date}, ${ship_date}, ${delivery_date}, ${tracking_number}, ${notes}, CURRENT_TIMESTAMP
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
      customer_id,
      item_code,
      quantity,
      total_value,
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
        customer_id = ${customer_id},
        item_code = ${item_code},
        quantity = ${quantity},
        total_value = ${total_value},
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

// Add: recent activity helper used by server
const getRecentOrderShipmentActivity = async (limit = 10) => {
  try {
    const sql = await database.sql();
    if (!sql) return [];
    const rows = await sql`
      SELECT id, order_id, product_name, status, updated_at
      FROM order_shipments
      ORDER BY updated_at DESC
      LIMIT ${limit}
    `;
    return rows;
  } catch (err) {
    console.error('Error fetching recent order shipment activity:', err);
    return [];
  }
};

// Update order shipment status (Mark Shipped with inventory deduction)
const updateOrderShipmentStatus = async (id, status, options = {}) => {
  try {
    const sql = await database.sql();
    const setShipDate = options.setShipDate ? options.setShipDate : null;
    const setDeliveryDate = options.setDeliveryDate ? options.setDeliveryDate : null;
    const willSetTracking = String(status || '').toLowerCase() === 'shipped';
    const generatedTracking = willSetTracking ? `TRCK${Math.floor(100000 + Math.random() * 900000)}` : null;

    // If marking as shipped, ensure sufficient inventory and deduct only on first transition
    if (String(status || '').toLowerCase() === 'shipped') {
      const rows = await sql`SELECT status, item_code, product_id, quantity FROM order_shipments WHERE id = ${id}`;
      if (!rows.length) throw new Error('Order not found');
      const current = rows[0];
      const currentStatus = String(current.status || '').toLowerCase();
      if (currentStatus !== 'shipped' && currentStatus !== 'delivered') {
        const requiredQty = Number(current.quantity) || 0;
        if (requiredQty > 0) {
          let invItem = null;
          if (current.item_code) {
            const itemsByCode = await sql`SELECT id, total_quantity FROM inventory_items WHERE item_code = ${current.item_code} LIMIT 1`;
            if (itemsByCode.length) invItem = itemsByCode[0];
          }
          if (!invItem && current.product_id) {
            const itemsByProduct = await sql`SELECT id, total_quantity FROM inventory_items WHERE product_id = ${current.product_id} ORDER BY total_quantity DESC LIMIT 1`;
            if (itemsByProduct.length) invItem = itemsByProduct[0];
          }
          if (!invItem) {
            throw new Error('you cannot ship the item');
          }
          const updatedStock = await sql`
            UPDATE inventory_items
            SET total_quantity = total_quantity - ${requiredQty},
                status = CASE WHEN total_quantity - ${requiredQty} <= 0 THEN 'out of stock' ELSE status END,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ${invItem.id} AND total_quantity >= ${requiredQty}
            RETURNING id, total_quantity
          `;
          if (!updatedStock.length) {
            throw new Error('you cannot ship the item');
          }
        }
      }
    }

    const result = await sql`
      UPDATE order_shipments
      SET status = ${status},
          ship_date = COALESCE(${setShipDate}, ship_date),
          delivery_date = COALESCE(${setDeliveryDate}, delivery_date),
          tracking_number = CASE WHEN ${willSetTracking} THEN COALESCE(tracking_number, ${generatedTracking}) ELSE tracking_number END,
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

module.exports = {
  initializeInventoryTable,
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
  getAllOrderShipments,
  getOrderShipmentById,
  createOrderShipment,
  updateOrderShipment,
  deleteOrderShipment,
  getOrderShipmentStats,
  updateOrderShipmentStatus,
  getStockOverview,
  getStockByCategory,
  getStockByWarehouse,
  getRecentOrderShipmentActivity
};
