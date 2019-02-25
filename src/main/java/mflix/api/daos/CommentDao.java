package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import mflix.api.models.Comment;
import mflix.api.models.Critic;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Indexes.descending;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Component
public class CommentDao extends AbstractMFlixDao {

    public static String COMMENT_COLLECTION = "comments";

    private MongoCollection<Comment> commentCollection;

    private CodecRegistry pojoCodecRegistry;

    private final Logger log;

    @Autowired
    public CommentDao(MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {

        super(mongoClient, databaseName);
        log = LoggerFactory.getLogger(this.getClass());

        this.db = this.mongoClient.getDatabase(MFLIX_DATABASE);

        this.pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        this.commentCollection = db.getCollection(COMMENT_COLLECTION, Comment.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Returns a Comment object that matches the provided id string.
     *
     * @param id - comment identifier
     * @return Comment object corresponding to the identifier value
     */
    public Comment getComment(String id) {
        return commentCollection.find(new Document("_id", new ObjectId(id))).first();
    }

    /**
     * Adds a new Comment to the collection. The equivalent instruction in the mongo shell would be:
     *
     * <p>db.comments.insertOne({comment})
     *
     * <p>
     *
     * @param comment - Comment object.
     * @throw IncorrectDaoOperation if the insert fails, otherwise
     * returns the resulting Comment object.
     */
    public Comment addComment(Comment comment) {

        // RESOLVED TODO> Ticket - Update User reviews: implement the functionality that enables adding a new
        // comment.

        if (comment != null) {

            try {

                commentCollection.withWriteConcern(WriteConcern.MAJORITY).insertOne(comment);

                Bson queryFilter = Filters.eq("_id", new ObjectId(comment.getId()));
                return commentCollection.find(queryFilter).first();
            } catch (Exception ex) {

                throw new IncorrectDaoOperation("Incorrect addComment method");
            }
        }

        // TODO> Ticket - Handling Errors: Implement a try catch block to
        // handle a potential write exception when given a wrong commentId.
        return null;
    }

    /**
     * Updates the comment text matching commentId and user email. This method would be equivalent to
     * running the following mongo shell command:
     *
     * <p>db.comments.update({_id: commentId}, {$set: { "text": text, date: ISODate() }})
     *
     * <p>
     *
     * @param commentId - comment id string value.
     * @param text      - comment text to be updated.
     * @param email     - user email.
     * @return true if successfully updates the comment text.
     */
    public boolean updateComment(String commentId, String text, String email) {

        // RESOLVED TODO> Ticket - Update User reviews: implement the functionality that enables updating an
        // user own comments

        try {

            Bson queryFilter = Filters.eq("_id", new ObjectId(commentId));
            Comment retrievedComment = commentCollection.find(queryFilter).first();

            if (retrievedComment != null && email != null && email.equals(retrievedComment.getEmail())) {

                Bson updateOperationDocument = Updates.combine(Updates.set("text", text), Updates.set("date", new Date()));

                return commentCollection.updateOne(queryFilter, updateOperationDocument).isModifiedCountAvailable();
            }
        } catch (Exception ex) {
            throw new IncorrectDaoOperation("Incorrect updateUserPreferences method");

        }
        // TODO> Ticket - Handling Errors: Implement a try catch block to
        // handle a potential write exception when given a wrong commentId.
        return false;
    }

    /**
     * Deletes comment that matches user email and commentId.
     *
     * @param commentId - commentId string value.
     * @param email     - user email value.
     * @return true if successful deletes the comment.
     */
    public boolean deleteComment(String commentId, String email) {

        // RESOLVED TODO> Ticket Delete Comments - Implement the method that enables the deletion of a user
        // comment
        // TIP: make sure to match only users that own the given commentId

        try {

            Bson queryFilter = Filters.eq("_id", new ObjectId(commentId));

            Comment retrievedComment = commentCollection.find(queryFilter).first();

            if (retrievedComment != null && email != null && email.equals(retrievedComment.getEmail())) {

                return commentCollection.deleteOne(queryFilter).wasAcknowledged();
            }

        } catch (Exception ex) {
            throw new IllegalArgumentException("Incorrect deleteComment method");
        }

        // TODO> Ticket Handling Errors - Implement a try catch block to
        // handle a potential write exception when given a wrong commentId.
        return false;
    }

    /**
     * Ticket: User Report - produce a list of users that comment the most in the website. Query the
     * `comments` collection and group the users by number of comments. The list is limited to up most
     * 20 commenter.
     *
     * @return List {@link Critic} objects.
     */
    public List<Critic> mostActiveCommenters() {

        List<Critic> mostActive = new ArrayList<>();

        // RESOLVED TODO> Ticket: User Report - execute a command that returns the
        // // list of 20 users, group by number of comments. Don't forget,
        // // this report is expected to be produced with an high durability
        // // guarantee for the returned documents. Once a commenter is in the
        // // top 20 of users, they become a Critic, so mostActive is composed of
        // // Critic objects.

        commentCollection.withReadConcern(ReadConcern.MAJORITY).aggregate(Arrays.asList(group("$email", sum("count", 1L)),
                sort(descending("count")), limit(20)), Critic.class)
                .iterator().forEachRemaining(mostActive::add);

        return mostActive;
    }
}