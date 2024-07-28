import json

from sqlite3 import connect


db = connect('items.db')
cr = db.cursor()


stmt = """CREATE TABLE IF NOT EXISTS areas (
    id INTEGER PRIMARY KEY,
    name TEXT,
    slug TEXT,
    fromMap BOOLEAN,
    ltd TEXT,
    lngt TEXT,
    displayName TEXT,
    ipse BOOLEAN
);
    CREATE TABLE IF NOT EXISTS stores (
        branchId TEXT PRIMARY KEY,
        branchName TEXT,
        restaurantId INTEGER,
        name TEXT,
        logo TEXT,
        description TEXT,
        latitude FLOAT,
        longitude FLOAT,
        totalReviews INTEGER,
        rate FLOAT,
        shopType INTEGER,
        isDarkStore BOOLEAN,
        isMigrated BOOLEAN,
        dhVendorId TEXT,
        deliveryTimeMins TEXT,
        deliveryFee FLOAT,
        minimumOrderValue INTEGER,
        areaId INTEGER,
        FOREIGN KEY (areaId) REFERENCES areas (id)
    );

    CREATE TABLE IF NOT EXISTS categories (
        id TEXT PRIMARY KEY,
        name TEXT
    );

    CREATE TABLE IF NOT EXISTS subcategories (
        id TEXT PRIMARY KEY,
        name TEXT
    );

    CREATE TABLE IF NOT EXISTS items (
        id TEXT PRIMARY KEY,
        dhVendorId TEXT,
        title TEXT,
        slug TEXT,
        description TEXT NULL,
        image TEXT,
        sku TEXT,
        categoryId TEXT,
        subcategoryId TEXT,
        FOREIGN KEY (subcategoryId) REFERENCES subcategories (id),
        FOREIGN KEY (categoryId) REFERENCES categories (id)
    );

    CREATE TABLE IF NOT EXISTS prices (
        originalPrice FLOAT,
        price FLOAT,
        discount FLOAT,
        discountPercentage FLOAT,
        itemId TEXT,
        storeId INTEGER,
        FOREIGN KEY (itemId) REFERENCES items (id),
        FOREIGN KEY (storeId) REFERENCES stores (branchId)
    );

    CREATE TABLE IF NOT EXISTS images (
        image TEXT,
        itemId TEXT,
        FOREIGN KEY (itemId) REFERENCES items (id)
    );
    
"""

cr.executescript(stmt)

with open('./items_with_categories.jsonl', 'r', encoding='utf-8') as file:
    for idx in range(1_000_000):
        line = file.readline()
        data: dict = json.loads(line)
        
        # areas
        area_id = data['store']['area']['id']
        
        stmt = "SELECT id FROM areas WHERE id=:area_id"
        cr.execute(stmt, {'area_id': area_id})
        
        results = cr.fetchall()
        if not results:
            stmt = "INSERT INTO areas (id, name, slug, fromMap, ltd, lngt, displayName, ipse) VALUES (:id, :name, :slug, :fromMap, :ltd, :lngt, :displayName, :ipse)"
            cr.execute(stmt, data['store']['area'])
            # db.commit()
            
        # store
        
        store_data = data['store']
        del store_data['area']
        
        stmt = "SELECT branchId FROM stores WHERE branchId=:branchId"
        cr.execute(stmt, {'branchId': store_data['branchId']})
        results = cr.fetchall()
        
        if not results:
            stmt = "INSERT INTO stores (branchName, branchId, restaurantId, name, logo, description, latitude, longitude, totalReviews, rate, shopType, isDarkStore, isMigrated, dhVendorId, deliveryTimeMins, deliveryFee, minimumOrderValue) VALUES (:branchName, :branchId, :restaurantId, :name, :logo, :description, :latitude, :longitude, :totalReviews, :rate, :shopType, :isDarkStore, :isMigrated, :dhVendorId, :deliveryTimeMins, :deliveryFee, :minimumOrderValue)"
            
            cr.execute(stmt, store_data)
            # db.commit()
        
        # category
        
        category_id = data['category']['id']
        stmt = 'SELECT id FROM categories WHERE id=:category_id'
        cr.execute(stmt, {'category_id': category_id})
        results = cr.fetchall()
        
        if not results:
            stmt = "INSERT INTO categories (id, name) VALUES (:id, :name)"
            cr.execute(stmt, data['category'])
            # db.commit()
        
        # subcategory
        
        subcategory_id = data['subcategory']['id']
        stmt = 'SELECT id FROM subcategories WHERE id=:subcategory_id'
        cr.execute(stmt, {'subcategory_id': subcategory_id})
        results = cr.fetchall()
        
        if not results:
            stmt = "INSERT INTO subcategories (id, name) VALUES (:id, :name)"
            cr.execute(stmt, data['subcategory'])
            # db.commit()
            
        # items
        
        item = data.copy()
        del item['store']
        del item['category']
        del item['subcategory']
        del item['attributes']
        del item['requestedQuantity']
        del item['originalPrice']
        del item['price']
        del item['discount']
        del item['discountPercentage']
        del item['images']
        
        stmt = "SELECT id FROM items WHERE id=:item_id"
        item_id = item['id']
        cr.execute(stmt, {'item_id': item_id})
        results = cr.fetchall()
        
        if not results:
            stmt = "INSERT INTO items (id, dhVendorId, title, slug, description, image, categoryId, subCategoryId, sku) VALUES (:id, :dhVendorId, :title, :slug, :description, :image, :categoryId, :subCategoryId, :sku)"
            
            cr.execute(stmt, {**item, "subCategoryId": subcategory_id, "categoryId": category_id, "storeId": store_data['branchId']})
            # db.commit()
            
        # prices
        
        price = {}
        price['originalPrice'] = data['originalPrice']
        price['price'] = data['price']
        price['discount'] = data['discount']
        price['discountPercentage'] = data['discountPercentage']
        
        stmt = "INSERT INTO prices (originalPrice, price, discount, discountPercentage, itemId, storeId) VALUES (:originalPrice, :price, :discount, :discountPercentage, :itemId, :storeId)"
        
        cr.execute(stmt, {**price, "itemId": item_id, "storeId": store_data['branchId']})
        # db.commit()
        
        images = data['images']
        for image in images:
            stmt = "INSERT INTO images (image, itemId) VALUES (:image, :itemId)"
            cr.execute(stmt, {"image": image, "itemId": item_id})
            
        db.commit()
        
        print(idx + 1)