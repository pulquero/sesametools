package net.fortytwo.sesametools.mappingsail;

import org.eclipse.rdf4j.model.IRI;

/**
 * Represents a rule to map an original IRI to a new IRI.
 * Rules are considered to be complete and self contained: MappingSail does not impose its own rewriting logic.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface RewriteRule {
    /**
     * @param original an complete IRI (i.e. not only a IRI prefix) to be rewritten
     * @return the resulting IRI
     */
    IRI rewrite(IRI original);
}
