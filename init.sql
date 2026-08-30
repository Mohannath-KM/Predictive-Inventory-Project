-- Core inventory storage tracking table
CREATE TABLE INVENTORYBIN (
    Id SERIAL PRIMARY KEY,  
    Item VARCHAR(50) NOT NULL,
    Inventory VARCHAR(50),
    BinNumber VARCHAR(50),
    Crate VARCHAR(50),
    Rack VARCHAR(50),
    CurrentCount INT DEFAULT 0,
    MinimumCount INT DEFAULT 10,  -- MIN_QTY_CRITICAL
    ReorderCount INT DEFAULT 50,  -- MIN_STOCK
    Project VARCHAR(50) DEFAULT 'Project X'
);

-- Inbound Tracking Ledger
CREATE TABLE CHECKIN_REQUEST (
    Id SERIAL PRIMARY KEY, 
    RequestID VARCHAR(50),
    RequesterUser VARCHAR(50),
    InventoryController VARCHAR(50),
    RequestDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CheckInType VARCHAR(50),
    State VARCHAR(50)
);

CREATE TABLE CHECKIN_ITEM (
    Id SERIAL PRIMARY KEY,
    CheckInRequestId INT REFERENCES CHECKIN_REQUEST(Id),
    Item VARCHAR(50) NOT NULL,
    Count INT NOT NULL,
    ReceivedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    InvoiceNumber VARCHAR(50),
    Comments TEXT,
    Vendor VARCHAR(100),
    UnitPrice DECIMAL(10,2),
    TotalPrice DECIMAL(10,2),
    CreatedBy VARCHAR(50),
    CurrencyId VARCHAR(10),
    CheckInDate TIMESTAMP
);

-- Outbound Tracking Ledger
CREATE TABLE CHECKOUT_REQUEST (
    Id SERIAL PRIMARY KEY,
    RequestId VARCHAR(50),
    RequesterUser VARCHAR(50),
    ReceiverUser VARCHAR(50),
    InventoryController VARCHAR(50),
    RequestDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CheckOutDate TIMESTAMP,
    CheckOutType VARCHAR(50), 
    CheckOutState VARCHAR(50),
    SubProject VARCHAR(50),
    CreatedByUserID VARCHAR(50)
);
 
CREATE TABLE CHECKOUT_ITEM (
    Id SERIAL PRIMARY KEY,
    CheckOutRequestId INT REFERENCES CHECKOUT_REQUEST(Id), -- FIXED: Changed VARCHAR(50) to INT
    Item VARCHAR(50) NOT NULL,
    Count INT NOT NULL,
    CollectDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    InventoryBinId INT REFERENCES INVENTORYBIN(Id)       -- FIXED: Changed VARCHAR(50) to INT
);

-- Insert dummy data for manual verification testing
-- FIXED: Added BinNumber to columns, fixed values ordering matching your columns
INSERT INTO INVENTORYBIN (Item, Inventory, BinNumber, CurrentCount, MinimumCount)
VALUES 
('ITEM-101', 'p4toolroom', 'B-12', 100, 15),
('ITEM-102', 'p4toolroom', 'B-14', 5, 20);


-- Predictive Reorder Recommendations Ledger
CREATE TABLE IF NOT EXISTS REORDER_RECOMMENDATION (
    Id SERIAL PRIMARY KEY,
    InventoryBinId INT REFERENCES INVENTORYBIN(Id),
    Item VARCHAR(50) NOT NULL,
    CurrentStock INT NOT NULL,
    DailyBurnRate DECIMAL(10,2),
    EstimatedDaysRemaining DECIMAL(10,2),
    SuggestedReorderQty INT NOT NULL,
    PriorityStatus VARCHAR(20) DEFAULT 'NORMAL', -- 'CRITICAL', 'WARNING', 'NORMAL'
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);