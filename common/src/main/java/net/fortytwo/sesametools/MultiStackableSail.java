
package net.fortytwo.sesametools;

import org.eclipse.rdf4j.IsolationLevel;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.StackableSail;

import java.io.File;
import java.util.List;

/**
 * A StackableSail which allows multiple Sails to be stacked upon the same base
 * Sail (avoiding re-initialization of the base Sail as the individual stacked
 * Sails are initialized)
 * 
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class MultiStackableSail implements StackableSail {
    private Sail baseSail;

    public MultiStackableSail(final Sail baseSail) {
        setBaseSail(baseSail);
    }

    @Override
    public void setBaseSail(Sail sail) {
        this.baseSail = sail;
    }

    @Override
    public Sail getBaseSail() {
        return baseSail;
    }

    @Override
    public void setDataDir(File file) {
        baseSail.setDataDir(file);
    }

    @Override
    public File getDataDir() {
        return baseSail.getDataDir();
    }

    @Override
    public void initialize() throws SailException {
        // Do nothing -- assume that the base Sail is initialized elsewhere
    }

    @Override
    public void shutDown() throws SailException {
        // Do nothing -- assume that the base Sail will be shut down elsewhere
    }

    @Override
    public boolean isWritable() throws SailException {
        return baseSail.isWritable();
    }

    @Override
    public SailConnection getConnection() throws SailException {
        return baseSail.getConnection();
    }

    @Override
    public ValueFactory getValueFactory() {
        return baseSail.getValueFactory();
    }

    @Override
    public List<IsolationLevel> getSupportedIsolationLevels() {
        return baseSail.getSupportedIsolationLevels();
    }

    @Override
    public IsolationLevel getDefaultIsolationLevel() {
        return baseSail.getDefaultIsolationLevel();
    }
}
