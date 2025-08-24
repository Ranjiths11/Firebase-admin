const express = require('express');
const admin = require('firebase-admin');
const cors = require('cors');

const app = express();

app.use(cors());
app.use(express.json());


   const serviceAccount = require('./firebaseServicekey.json');


admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

// Store active SSE connections (will reset on each deployment)
const activeConnections = new Map();

async function fetchUserWithGroups(userId) {
  try {
    const userDoc = await db.collection('users').doc(userId).get();
    
    if (!userDoc.exists) {
      throw new Error('User not found');
    }

    const userData = userDoc.data();
    
    const groupPromises = userData.group.map(async (groupName) => {
      try {
        const groupDoc = await db.collection('groups').doc(groupName).get();
        
        if (groupDoc.exists) {
          return {
            id: groupDoc.id,
            groupName: groupName,
            ...groupDoc.data()
          };
        } else {
          return {
            id: groupName,
            groupName: groupName,
            isActive: null,
            createdAt: null,
            exists: false
          };
        }
      } catch (error) {
        console.error(`Error fetching group ${groupName}:`, error);
        return {
          id: groupName,
          groupName: groupName,
          error: error.message,
          exists: false
        };
      }
    });

    const groups = await Promise.all(groupPromises);

    return {
      success: true,
      userId: userDoc.id,
      user: userData,
      groups: groups,
      timestamp: new Date().toISOString()
    };

  } catch (error) {
    console.error('Error fetching user data:', error);
    throw error;
  }
}

// Setup real-time listeners for a user
function setupRealtimeListeners(userId, res) {
  const userRef = db.collection('users').doc(userId);
  
  // Listen to user document changes
  const unsubscribeUser = userRef.onSnapshot(async (doc) => {
    try {
      if (doc.exists) {
        console.log(`User ${userId} data changed, sending update...`);
        const result = await fetchUserWithGroups(userId);
        
        // Send SSE update
        res.write(`data: ${JSON.stringify({
          type: 'userUpdate',
          data: result
        })}\n\n`);
      }
    } catch (error) {
      console.error(`Error in user listener for ${userId}:`, error);
      res.write(`data: ${JSON.stringify({
        type: 'error',
        error: error.message,
        userId: userId
      })}\n\n`);
    }
  });

  // Get user's groups and listen to group changes
  fetchUserWithGroups(userId).then(userData => {
    const groupUnsubscribes = userData.user.group.map(groupName => {
      const groupRef = db.collection('groups').doc(groupName);
      
      return groupRef.onSnapshot(async (doc) => {
        try {
          console.log(`Group ${groupName} changed, sending update for user ${userId}...`);
          const result = await fetchUserWithGroups(userId);
          
          // Send SSE update
          res.write(`data: ${JSON.stringify({
            type: 'groupUpdate',
            data: result,
            changedGroup: groupName
          })}\n\n`);
        } catch (error) {
          console.error(`Error in group listener for ${groupName}:`, error);
          res.write(`data: ${JSON.stringify({
            type: 'error',
            error: error.message,
            userId: userId,
            groupName: groupName
          })}\n\n`);
        }
      });
    });

    // Store all unsubscribe functions
    return { userUnsubscribe: unsubscribeUser, groupUnsubscribes };
  }).then(unsubscribeFunctions => {
    const connectionId = `${userId}-${Date.now()}`;
    activeConnections.set(connectionId, {
      userId: userId,
      response: res,
      ...unsubscribeFunctions,
      createdAt: new Date()
    });

    // Clean up when connection closes
    res.on('close', () => {
      console.log(`SSE connection closed for user ${userId}`);
      const connection = activeConnections.get(connectionId);
      if (connection) {
        connection.userUnsubscribe();
        connection.groupUnsubscribes.forEach(unsubscribe => unsubscribe());
        activeConnections.delete(connectionId);
      }
    });
  });
}

// REST API: Get user data (one-time) - MAIN ENDPOINT FOR RENDER
app.get('/api/user/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    
    console.log(`REST API request for user ID: ${userId}`);
    
    const result = await fetchUserWithGroups(userId);
    
    res.json(result);
    
  } catch (error) {
    console.error('API Error:', error);
    res.status(404).json({ 
      success: false,
      error: error.message || 'User not found',
      timestamp: new Date().toISOString()
    });
  }
});

// POLLING endpoint for pseudo-realtime (Render-friendly alternative)
app.get('/api/user/:userId/poll', async (req, res) => {
  try {
    const { userId } = req.params;
    const { lastUpdated } = req.query;
    
    console.log(`Polling request for user ID: ${userId}, lastUpdated: ${lastUpdated}`);
    
    const result = await fetchUserWithGroups(userId);
    
    // Add polling-friendly metadata
    res.json({
      ...result,
      serverTime: new Date().toISOString(),
      pollInterval: 5000, // Suggest 5-second polling
      isNewData: !lastUpdated || new Date(result.timestamp) > new Date(lastUpdated)
    });
    
  } catch (error) {
    console.error('Polling API Error:', error);
    res.status(404).json({ 
      success: false,
      error: error.message || 'User not found',
      timestamp: new Date().toISOString()
    });
  }
});

// SSE API: Real-time updates (LIMITED ON RENDER FREE)
app.get('/api/user/:userId/realtime', async (req, res) => {
  try {
    const { userId } = req.params;
    
    console.log(`SSE connection requested for user ID: ${userId}`);
    
    // Set SSE headers
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Cache-Control'
    });

    // Send initial data
    const initialData = await fetchUserWithGroups(userId);
    res.write(`data: ${JSON.stringify({
      type: 'initial',
      data: initialData
    })}\n\n`);

    // Send connection confirmation with warning about Render limitations
    res.write(`data: ${JSON.stringify({
      type: 'connected',
      message: 'Real-time connection established (Note: May disconnect after 15 minutes on free tier)',
      userId: userId,
      timestamp: new Date().toISOString(),
      warning: 'For reliable updates, consider using polling endpoint /api/user/{userId}/poll'
    })}\n\n`);

    // Setup real-time listeners
    setupRealtimeListeners(userId, res);

    // REDUCED heartbeat - every 5 minutes instead of 30 seconds
    const heartbeatInterval = setInterval(() => {
      res.write(`data: ${JSON.stringify({
        type: 'heartbeat',
        timestamp: new Date().toISOString(),
        uptime: process.uptime()
      })}\n\n`);
    }, 300000); // Every 5 minutes

    res.on('close', () => {
      clearInterval(heartbeatInterval);
    });
    
  } catch (error) {
    console.error('SSE API Error:', error);
    res.status(404).json({ 
      success: false,
      error: error.message || 'User not found',
      timestamp: new Date().toISOString()
    });
  }
});

// Keep-alive endpoint to prevent sleeping (call this from frontend every 10 minutes)
app.get('/api/keep-alive', (req, res) => {
  res.json({ 
    status: 'alive',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    message: 'Server is awake'
  });
});

app.get('/api/connections', (req, res) => {
  const connections = Array.from(activeConnections.entries()).map(([id, conn]) => ({
    connectionId: id,
    userId: conn.userId,
    createdAt: conn.createdAt,
    uptime: Date.now() - conn.createdAt.getTime()
  }));

  res.json({
    success: true,
    totalConnections: activeConnections.size,
    connections: connections,
    timestamp: new Date().toISOString()
  });
});

app.delete('/api/connection/:userId', (req, res) => {
  const { userId } = req.params;
  let closedCount = 0;

  for (const [connectionId, connection] of activeConnections.entries()) {
    if (connection.userId === userId) {
      connection.userUnsubscribe();
      connection.groupUnsubscribes.forEach(unsubscribe => unsubscribe());
      connection.response.end();
      activeConnections.delete(connectionId);
      closedCount++;
    }
  }

  res.json({
    success: true,
    message: `Closed ${closedCount} connections for user ${userId}`,
    timestamp: new Date().toISOString()
  });
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK',
    service: 'Mobile Real-time User Data API (Render Optimized)',
    activeConnections: activeConnections.size,
    uptime: process.uptime(),
    environment: process.env.NODE_ENV || 'development',
    timestamp: new Date().toISOString() 
  });
});

app.get('/', (req, res) => {
  res.json({
    message: 'Real-time User Data API (Render Free Tier Optimized)',
    deployment: {
      platform: 'Render Free Tier',
      limitations: [
        'Apps sleep after 15 minutes of inactivity',
        'Cold starts take 30+ seconds',
        'SSE connections break when app sleeps'
      ],
      recommendations: [
        'Use polling endpoint for reliable updates',
        'Call keep-alive endpoint every 10 minutes',
        'Implement reconnection logic in frontend'
      ]
    },
    endpoints: {
      oneTimeUserData: {
        method: 'GET',
        url: '/api/user/{userId}',
        description: 'Get user data once (REST API) - RECOMMENDED FOR RENDER'
      },
      pollingUserData: {
        method: 'GET',
        url: '/api/user/{userId}/poll?lastUpdated={timestamp}',
        description: 'Get user data with polling support - RENDER FRIENDLY'
      },
      realtimeUserData: {
        method: 'GET',
        url: '/api/user/{userId}/realtime',
        description: 'Get user data with real-time updates (SSE) - LIMITED ON RENDER FREE'
      },
      keepAlive: {
        method: 'GET',
        url: '/api/keep-alive',
        description: 'Keep server awake (call every 10 minutes from frontend)'
      },
      connections: {
        method: 'GET',
        url: '/api/connections',
        description: 'Get all active real-time connections'
      },
      closeConnection: {
        method: 'DELETE',
        url: '/api/connection/{userId}',
        description: 'Close real-time connection for a user'
      },
      healthCheck: {
        method: 'GET',
        url: '/health',
        description: 'API health status'
      }
    },
    usage: {
      recommended: 'Use /api/user/{userId}/poll for pseudo-realtime updates',
      alternative: 'Use /api/user/{userId} for one-time requests',
      realtime: 'Use /api/user/{userId}/realtime with caution (may disconnect)'
    },
    activeConnections: activeConnections.size,
    timestamp: new Date().toISOString()
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Mobile Real-time API Server running on port ${PORT}`);
  console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
  console.log(`Platform: ${process.env.RENDER ? 'Render' : 'Local'}`);
  console.log(`One-time API: /api/user/{userId}`);
  console.log(`Polling API: /api/user/{userId}/poll`);
  console.log(`Real-time API: /api/user/{userId}/realtime`);
  console.log(`Keep-alive: /api/keep-alive`);
  console.log(`Health Check: /health`);
});

process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down gracefully');
  
  // Clean up all active connections
  for (const [connectionId, connection] of activeConnections.entries()) {
    connection.userUnsubscribe();
    connection.groupUnsubscribes.forEach(unsubscribe => unsubscribe());
    connection.response.end();
  }
  activeConnections.clear();
  
  process.exit(0);
});

module.exports = app;