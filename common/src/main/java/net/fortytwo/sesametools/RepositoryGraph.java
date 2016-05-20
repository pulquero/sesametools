package net.fortytwo.sesametools;

import java.util.Collection;
import java.util.Iterator;

import org.eclipse.rdf4j.model.Graph;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;

/**
 * An adapter which wraps a RepositoryConnection as a Graph
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class RepositoryGraph implements Graph {
    private static final boolean INFER = false;

    private final RepositoryConnection rc;

    public RepositoryGraph(final RepositoryConnection rc) throws RepositoryException {
        this.rc = rc;
    }

    public ValueFactory getValueFactory() {
        return rc.getValueFactory();
    }

    public boolean add(Resource s, IRI p, Value o, Resource... c) {
        try {
            rc.begin();
            rc.add(s, p, o, c);
            rc.commit();
            return true;
        } catch (RepositoryException e) {
            throw new RepositoryGraphRuntimeException(e);
        }
    }

    // note: the returned iterator contains a CloseableIteration which will not be closed
    public Iterator<Statement> match(Resource s, IRI p, Value o, Resource... c) {
        RepositoryResult<Statement> result = null;
        try {
            result = rc.getStatements(s, p, o, INFER, c);
        } catch (RepositoryException e) {
            throw new RepositoryGraphRuntimeException(e);
        }
        return new RepositoryResultIterator(result);
    }

    public int size() {
        try {
            return (int) rc.size();
        } catch (RepositoryException e) {
            throw new RepositoryGraphRuntimeException(e);
        }
    }

    public boolean isEmpty() {
        return 0 == size();
    }

    public boolean contains(Object o) {
        if (o instanceof Statement) {
            Statement st = (Statement) o;
            try {
                RepositoryResult result = rc.getStatements(
                        st.getSubject(), st.getPredicate(), st.getObject(), INFER, st.getContext());
                try {
                    return result.hasNext();
                } finally {
                    result.close();
                }
            } catch (Exception e) {
                throw new RepositoryGraphRuntimeException(e);
            }
        } else {
            return false;
        }
    }

    public Iterator<Statement> iterator() {
        return match(null, null, null);
    }

    public Object[] toArray() {
        int size = size();
        Object[] a = new Object[size];
        if (size > 0) {
            try {
                int i = 0;
                RepositoryResult result = rc.getStatements(null, null, null, INFER);
                try {
                    while (result.hasNext()) {
                        a[i] = result.next();
                        i++;
                    }
                } finally {
                    result.close();
                }
            } catch (Exception e) {
                throw new RepositoryGraphRuntimeException(e);
            }
        }

        return a;
    }

    public <T> T[] toArray(T[] ts) {
        // TODO: only Statement is acceptable as T
        return (T[]) toArray();
    }

    public boolean add(Statement statement) {
        try {
            rc.add(statement);
        } catch (RepositoryException e) {
            throw new RepositoryGraphRuntimeException(e);
        }

        // the RepositoryConnection API does not provide an efficient means
        // of knowing whether the repository was changed
        return false;
    }

    public boolean remove(Object o) {
        if (o instanceof Statement) {
            Statement st = (Statement) o;
            try {
                rc.remove(st.getSubject(), st.getPredicate(), st.getObject(), st.getContext());
            } catch (RepositoryException e) {
                throw new RepositoryGraphRuntimeException(e);
            }
        }
        // the RepositoryConnection API does not provide an efficient means of knowing whether a statement was removed
        return false;
    }

    public boolean containsAll(Collection<?> objects) {
        for (Object o : objects) {
            if (!contains(o)) {
                return false;
            }
        }

        return true;
    }

    public boolean addAll(Collection<? extends Statement> statements) {
        for (Statement s : statements) {
            add(s);
        }

        return false;
    }

    public boolean removeAll(Collection<?> objects) {
        for (Object o : objects) {
            remove(o);
        }

        return false;
    }

    public boolean retainAll(Collection<?> objects) {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        try {
            rc.clear();
        } catch (RepositoryException e) {
            throw new RepositoryGraphRuntimeException(e);
        }
    }

    private class RepositoryResultIterator implements Iterator<Statement> {
        private final RepositoryResult result;

        private RepositoryResultIterator(RepositoryResult result) {
            this.result = result;
        }

        public boolean hasNext() {
            try {
                return result.hasNext();
            } catch (RepositoryException e) {
                throw new RepositoryGraphRuntimeException(e);
            }
        }

        public Statement next() {
            try {
                return (Statement) result.next();
            } catch (RepositoryException e) {
                throw new RepositoryGraphRuntimeException(e);
            }
        }

        public void remove() {
            try {
                result.remove();
            } catch (RepositoryException e) {
                throw new RepositoryGraphRuntimeException(e);
            }
        }
    }

    public class RepositoryGraphRuntimeException extends RuntimeException {
        public RepositoryGraphRuntimeException(final Throwable cause) {
            super(cause);
        }
    }
}
