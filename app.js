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


app.get('/api/user/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    
    console.log(`API request for user ID: ${userId}`);
    
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


app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK',
    service: 'User Data API',
    timestamp: new Date().toISOString() 
  });
});


app.get('/', (req, res) => {
  res.json({
    message: 'User Data API',
    endpoints: {
      getUserData: 'GET /api/user/{userId}',
      healthCheck: 'GET /health'
    },
    usage: 'Send GET request to /api/user/{userId} to get user and group data',
    timestamp: new Date().toISOString()
  });
});


const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 API Server running on port ${PORT}`);
  console.log(`📡 API Endpoint: http://localhost:${PORT}/api/user/{userId}`);
  console.log(`🔍 Health Check: http://localhost:${PORT}/health`);
});


process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down gracefully');
  process.exit(0);
});

module.exports = app;