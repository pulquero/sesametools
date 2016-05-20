package net.fortytwo.sesametools.mappingsail;

import org.eclipse.rdf4j.model.IRI;

import java.util.HashMap;
import java.util.Map;

/**
 * A set of rules for rewriting IRIs based on direction (to or from the data store)
 * and part of speech (the position in an RDF statement in which a IRI appears)
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class MappingSchema {
    /**
     * The position in an RDF statement in which a IRI appears
     */
    public enum PartOfSpeech {
        SUBJECT, PREDICATE, OBJECT, CONTEXT
    }

    /**
     * The direction of a IRI rewriting rule: inbound rules map externally visible IRIs
     * to the IRIs found in the data store, while outbound rules do the opposite.
     */
    public enum Direction {
        INBOUND, OUTBOUND
    }

    private final RewriteRule defaultRewriter = new RewriteRule() {
        public IRI rewrite(final IRI original) {
            return original;
        }
    };

    private final Map<String, RewriteRule> rewriters
            = new HashMap<String, RewriteRule>();

    /**
     * @param partOfSpeech the position in an RDF statement (subject, predicate, object or context)
     *                     in which the IRI appears
     * @param direction    whether this is a rule to map externally visible IRIs to internal IRIs (inbound)
     *                     or the reverse (outbound)
     * @return the matching rewriting rule.
     *         If no such rule has been explicitly defined, the default rule (the identity mapping) is returned.
     */
    public RewriteRule getRewriter(final PartOfSpeech partOfSpeech,
                                   final Direction direction) {
        RewriteRule r = rewriters.get("" + partOfSpeech + direction);
        return null == r
                ? defaultRewriter
                : r;
    }

    /**
     * Defines an inbound or outbound IRI rewriter.
     *
     * @param direction whether this is a rule to map externally visible IRIs to internal IRIs (inbound)
     *                  or the reverse (outbound)
     * @param rule      the rewriting rule
     */
    public void setRewriter(final Direction direction,
                            final RewriteRule rule) {
        for (PartOfSpeech p : PartOfSpeech.values()) {
            setRewriter(direction, p, rule);
        }
    }

    /**
     * Defines an inbound or outbound IRI rewriter for a specific part of speech
     * (subject, predicate, object, or context).
     *
     * @param partOfSpeech the part of speech to which this rewriter applies
     * @param direction    whether this is a rule to map externally visible IRIs to internal IRIs (inbound)
     *                     or the reverse (outbound)
     * @param rule         the rewriting rule
     */
    public void setRewriter(final Direction direction,
                            final PartOfSpeech partOfSpeech,
                            final RewriteRule rule) {
        rewriters.put("" + partOfSpeech + direction, rule);
    }
}
