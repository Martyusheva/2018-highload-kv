package ru.mail.polis.martyusheva;

import ru.mail.polis.KVDao;

import java.io.*;
import java.util.*;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

import static org.iq80.leveldb.impl.Iq80DBFactory.bytes;

/**
 * Created by moresmart on 13.12.18.
 */
public class KVDaoImpl implements KVDao{

    private final DB db;

    public KVDaoImpl(final File dir) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        db = Iq80DBFactory.factory.open(dir, options);
    }

    @Override
    public byte[] get(byte[] key) throws IOException, NoSuchElementException {
        byte[] value = db.get(key);
        if (value == null) {
            throw new NoSuchElementException();
        }

        return value;
    }

    @Override
    public void upsert(byte[] key, byte[] value) throws IOException {
        db.put(key, value);
    }

    @Override
    public void remove(byte[] key) {
        db.delete(key);
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

}
