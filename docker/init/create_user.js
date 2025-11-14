db = db.getSiblingDB('admin');

const user = process.env.MONGO_INITDB_ROOT_USERNAME ;
const pwd  = process.env.MONGO_INITDB_ROOT_PASSWORD ;
const appDb = process.env.MONGO_INITDB_DATABASE ;

// Il s'agit du root user
const exists = db.getUser(user);
if (!exists) {
  db.createUser({
    user: user,
    pwd:  pwd, // MongoDB le transformera en secret stocké de manière sécurisée 
    // dans la collection system.users de la base admin, 
    // selon son mécanisme d’authentification configuré
    roles: [{ role: 'readWrite', db: appDb }]
  });
} else {
  // Optionnel: met à jour le mot de passe/roles si besoin
  // db.updateUser(user, { pwd: pwd, roles: [{ role: 'readWrite', db: appDb }] });
}

if (!db.getUser('admin')) {
  db.createUser({
    user: 'admin',
    pwd: 'password123',
    roles: [{ role: 'userAdmin', db: appDb }]
  });
}

if (!db.getUser('readerwritter')) {
  db.createUser({
    user: 'readerwritter',
    pwd: 'readerpass',
    roles: [{ role: 'readWrite', db: appDb }]
  });
}
