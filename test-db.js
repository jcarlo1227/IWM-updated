#!/usr/bin/env node

// Simple database connection test script
// Run with: node test-db.js

require('dotenv').config();

async function runTests() {
  console.log('🧪 Database Connection Test Suite');
  console.log('=====================================\n');
  
  // Test 1: Environment variables
  console.log('1️⃣ Environment Variables Check:');
  console.log(`   DATABASE_URL: ${process.env.DATABASE_URL ? '✅ Set' : '❌ Not set'}`);
  console.log(`   NODE_ENV: ${process.env.NODE_ENV || 'development'}`);
  
  if (process.env.DATABASE_URL) {
    console.log(`   URL Length: ${process.env.DATABASE_URL.length} characters`);
    console.log(`   Format: ${process.env.DATABASE_URL.includes('neon') ? 'Neon' : 'Other'}`);
    
    try {
      const url = new URL(process.env.DATABASE_URL);
      console.log(`   Host: ${url.hostname}`);
      console.log(`   Port: ${url.port || '5432 (default)'}`);
      console.log(`   Database: ${url.pathname.substring(1)}`);
    } catch (error) {
      console.log(`   ❌ URL parsing failed: ${error.message}`);
    }
  }
  
  console.log('');
  
  // Test 2: Network connectivity
  console.log('2️⃣ Network Connectivity Test:');
  try {
    const { testNetworkConnectivity } = require('./database');
    const networkOk = await testNetworkConnectivity();
    console.log(`   Result: ${networkOk ? '✅ Connected' : '❌ Failed'}`);
  } catch (error) {
    console.log(`   ❌ Network test error: ${error.message}`);
  }
  
  console.log('');
  
  // Test 3: Database connection
  console.log('3️⃣ Database Connection Test:');
  try {
    const { testDatabaseConnection } = require('./database');
    const dbOk = await testDatabaseConnection();
    console.log(`   Result: ${dbOk ? '✅ Connected' : '❌ Failed'}`);
  } catch (error) {
    console.log(`   ❌ Database test error: ${error.message}`);
  }
  
  console.log('');
  
  // Test 4: Database status
  console.log('4️⃣ Database Status Check:');
  try {
    const { getDatabaseStatus, isDatabaseAvailable } = require('./database');
    const status = getDatabaseStatus();
    const available = isDatabaseAvailable();
    
    console.log(`   Available: ${available ? '✅ Yes' : '❌ No'}`);
    console.log(`   Has URL: ${status.hasUrl ? '✅ Yes' : '❌ No'}`);
    console.log(`   Connection Promise: ${status.connectionPromise ? '✅ Yes' : '❌ No'}`);
  } catch (error) {
    console.log(`   ❌ Status check error: ${error.message}`);
  }
  
  console.log('\n=====================================');
  console.log('🏁 Test suite completed');
}

// Run the tests
runTests().catch(error => {
  console.error('❌ Test suite failed:', error);
  process.exit(1);
});