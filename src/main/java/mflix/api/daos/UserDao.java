package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    private final MongoCollection<User> usersCollection;
    // RESOLVED TODO> Ticket: User Management - do the necessary changes so that the sessions collection
    //returns a Session object
    private final MongoCollection<Session> sessionsCollection;

    private final Logger log;

    @Autowired
    public UserDao(MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {

        super(mongoClient, databaseName);

        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);

        log = LoggerFactory.getLogger(this.getClass());

        // RESOLVED TODO> Ticket: User Management - implement the necessary changes so that the sessions
        // collection returns a Session objects instead of Document objects.

        sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {

        // RESOLVED TODO > Ticket: Durable Writes -  you might want to use a more durable write concern here!

        if (usersCollection.find(Filters.eq("email", user.getEmail())).first() != null) {
            throw new IncorrectDaoOperation("Incorrect addUser method, user already exists!");
        } else {
            usersCollection.withWriteConcern(WriteConcern.MAJORITY).insertOne(user);
        }

        // RESOLVED TODO > Ticket: Handling Errors - make sure to only add new users
        // and not users that already exist.

        return usersCollection.find(Filters.eq("email", user.getEmail())).first() != null;
    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {

        // RESOLVED TODO> Ticket: User Management - implement the method that allows session information to be
        // stored in it's designated collection.

        Session session = new Session();
        session.setUserId(userId);
        session.setJwt(jwt);

        Bson queryFilter = Filters.and(eq("user_id", userId));

        Session oldSession = sessionsCollection.find(queryFilter).first();

        if (oldSession != null) {
            sessionsCollection.deleteMany(queryFilter);
        }
        sessionsCollection.insertOne(session);
        return true;

        // RESOLVED TODO > Ticket: Handling Errors - implement a safeguard against
        // creating a session with the same jwt token.
    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {
        // RESOLVED TODO> Ticket: User Management - implement the query that returns the first User object.
        return usersCollection.find(Filters.eq("email", email)).first();
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {
        // RESOLVED TODO> Ticket: User Management - implement the method that returns Sessions for a given
        // userId

        return sessionsCollection.find(Filters.eq("user_id", userId)).first();
    }

    public boolean deleteUserSessions(String userId) {
        // RESOLVED TODO> Ticket: User Management - implement the delete user sessions method
        DeleteResult deleteResult = sessionsCollection.deleteMany(Filters.eq("user_id", userId));
        return deleteResult.wasAcknowledged();
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        // remove user sessions
        // RESOLVED TODO> Ticket: User Management - implement the delete user method
        try {

            deleteUserSessions(email);
            DeleteResult deleteResult = usersCollection.deleteMany(Filters.eq("email", email));
            return deleteResult.wasAcknowledged();
        } catch (MongoException ex) {
            return false;
        }
        // RESOLVED TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions.
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
        // RESOLVED TODO> Ticket: User Preferences - implement the method that allows for user preferences to
        // be updated.

        try {

            if (userPreferences != null) {
                Bson queryFilter = Filters.eq("email", email);
                return usersCollection.updateOne(queryFilter, set("preferences", userPreferences)).isModifiedCountAvailable();
            }
            return false;
        } catch (MongoException ex) {
            throw new IncorrectDaoOperation("Incorrect updateUserPreferences method");
        }
        // RESOLVED TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions when updating an entry.
    }
}
