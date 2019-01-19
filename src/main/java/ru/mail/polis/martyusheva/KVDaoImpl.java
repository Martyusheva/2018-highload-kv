package ru.mail.polis.martyusheva;

import org.iq80.leveldb.impl.Iq80DBFactory;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.*;
import java.util.*;

import org.iq80.leveldb.*;

public class KVDaoImpl implements KVDao{

    private final DB db;

    public KVDaoImpl(@NotNull final File dir) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        db = Iq80DBFactory.factory.open(dir, options);
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException {
        byte[] value = db.get(key);
        if (value == null) {
            throw new NoSuchElementException();
        }

        return value;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        db.put(key, value);
    }

    @Override
    public void remove(@NotNull byte[] key) {
       db.delete(key);
    }

    @Override
    public void close() throws IOException {
        db.close();
    }
}
