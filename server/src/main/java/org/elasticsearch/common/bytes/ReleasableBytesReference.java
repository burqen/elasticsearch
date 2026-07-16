/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An extension to {@link BytesReference} that requires releasing its content. This
 * class exists to make it explicit when a bytes reference needs to be released, and when not.
 */
public final class ReleasableBytesReference implements RefCounted, Releasable, BytesReference {
    static final AtomicLong NEXT_ID = new AtomicLong(0);
    final long id = NEXT_ID.getAndIncrement();

    public long getId() {
        return id;
    }

    Logger logger = LogManager.getLogger(getClass());

    private static final ReleasableBytesReference EMPTY = new ReleasableBytesReference(BytesArray.EMPTY, RefCounted.ALWAYS_REFERENCED);

    private BytesReference delegate;
    private final RefCounted refCounted;

    public static ReleasableBytesReference empty() {
        return EMPTY;
    }

    public ReleasableBytesReference(BytesReference delegate, Releasable releasable) {
        this(delegate, new RefCountedReleasable(releasable));
    }

    public ReleasableBytesReference(BytesReference delegate, RefCounted refCounted) {
        this.delegate = delegate;
        this.refCounted = refCounted;
        assert refCounted.hasReferences();
    }

    public static ReleasableBytesReference wrap(BytesReference reference) {
        assert reference instanceof ReleasableBytesReference == false : "use #retain() instead of #wrap() on a " + reference.getClass();
        return reference.length() == 0 ? empty() : new ReleasableBytesReference(reference, ALWAYS_REFERENCED);
    }

    /**
     * Take shared ownership of pooled bytes reachable from {@code reference} without copying the payload.
     * <p>
     * Retains {@link ReleasableBytesReference} leaves. For {@link CompositeBytesReference}, recursively adopts each
     * component and ties a single release to all retained parts. Plain heap references (e.g. {@link BytesArray}) use
     * {@link #wrap(BytesReference)} (no native buffer to release).
     */
    public static ReleasableBytesReference adopt(BytesReference reference) {
        Objects.requireNonNull(reference);
        if (reference.length() == 0) {
            return empty();
        }
        if (reference instanceof ReleasableBytesReference r) {
            return r.retain();
        }
        if (reference instanceof CompositeBytesReference composite) {
            final BytesReference[] parts = composite.componentReferences();
            final ReleasableBytesReference[] retained = new ReleasableBytesReference[parts.length];
            int filled = 0;
            try {
                for (BytesReference part : parts) {
                    retained[filled++] = adopt(part);
                }
            } catch (RuntimeException e) {
                for (int j = 0; j < filled; j++) {
                    Releasables.close(retained[j]);
                }
                throw e;
            }
            BytesReference joined = CompositeBytesReference.of(retained);
            return new ReleasableBytesReference(joined, () -> Releasables.close(retained));
        }
        return wrap(reference);
    }

    public static BytesReference unwrap(BytesReference reference) {
        if (reference instanceof ReleasableBytesReference releasable) {
            return releasable.delegate;
        }
        return reference;
    }

    @Override
    public void incRef() {
        refCounted.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refCounted.tryIncRef();
    }

    @Override
    public boolean decRef() {
        boolean release = refCounted.decRef();
        Throwable stackTrace = new Throwable();
        logger.trace(
            "decRef source=[{}], release=[{}], thread=[{}] stackTrace={}",
            this,
            release,
            Thread.currentThread().getName(),
            ExceptionsHelper.stackTrace(stackTrace)
        );
        if (release) {
            delegate = null;
        }
        return release;
    }

    @Override
    public boolean hasReferences() {
        boolean hasRef = refCounted.hasReferences();
        // delegate is nulled out when the ref-count reaches zero but only via a plain store, and also we could be racing with a concurrent
        // decRef so need to check #refCounted again in case we run into a non-null delegate but saw a reference before
        assert delegate != null || refCounted.hasReferences() == false;
        return hasRef;
    }

    public ReleasableBytesReference retain() {
        refCounted.mustIncRef();
        return this;
    }

    /**
     * Same as {@link #slice} except that the slice is not guaranteed to share the same underlying reference count as this instance.
     * This method is equivalent to calling {@code .slice(from, length).retain()} but might be more efficient through the avoidance of
     * retaining unnecessary buffers.
     */
    public ReleasableBytesReference retainedSlice(int from, int length) {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        if (from == 0 && length() == length) {
            return retain();
        }
        final BytesReference slice = delegate.slice(from, length);
        if (slice instanceof ReleasableBytesReference releasable) {
            return releasable.retain();
        }
        refCounted.incRef();
        return new ReleasableBytesReference(slice, refCounted);
    }

    @Override
    public void close() {
        boolean release = refCounted.decRef();
        Throwable stackTrace = new Throwable();
        String s = ExceptionsHelper.stackTrace(stackTrace);
        s = s.contains("org.elasticsearch.index.translog.TranslogWriter.writeAndReleaseOps")
            ? "org.elasticsearch.index.translog.TranslogWriter.writeAndReleaseOps"
            : s;
        logger.trace(
            "close -> decRef source=[{}], release=[{}], thread=[{}] stackTrace={}",
            this,
            release,
            Thread.currentThread().getName(),
            s
        );
    }

    @Override
    public byte get(int index) {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.get(index);
    }

    @Override
    public int getInt(int index) {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.getInt(index);
    }

    @Override
    public int getIntLE(int index) {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.getIntLE(index);
    }

    @Override
    public long getLongLE(int index) {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.getLongLE(index);
    }

    @Override
    public double getDoubleLE(int index) {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.getDoubleLE(index);
    }

    @Override
    public int indexOf(byte marker, int from) {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.indexOf(marker, from);
    }

    @Override
    public int length() {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.length();
    }

    /**
     * {@inheritDoc}
     *
     * The returned bytes reference will share the reference count of this instance and as such any ref-counting operations on the return
     * are shared with this instance and vice versa. Using {@link #retainedSlice(int, int)} might be more efficient in situations where the
     * return of this method is subsequently retained by increasing its ref-count.
     */
    @Override
    public ReleasableBytesReference slice(int from, int length) {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return new ReleasableBytesReference(delegate.slice(from, length), refCounted);
    }

    @Override
    public long ramBytesUsed() {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.ramBytesUsed();
    }

    @Override
    public StreamInput streamInput() throws IOException {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return new BytesReferenceStreamInput(delegate) {
            private ReleasableBytesReference retainAndSkip(int len) throws IOException {
                if (len == 0) {
                    return ReleasableBytesReference.empty();
                }
                // instead of reading the bytes from a stream we just create a slice of the underlying bytes
                final ReleasableBytesReference result = retainedSlice(offset(), len);
                // move the stream manually since creating the slice didn't move it
                skip(len);
                return result;
            }

            @Override
            public ReleasableBytesReference readReleasableBytesReference() throws IOException {
                final int len = readVInt();
                return retainAndSkip(len);
            }

            @Override
            public ReleasableBytesReference readReleasableBytesReference(int len) throws IOException {
                return retainAndSkip(len);
            }

            @Override
            public ReleasableBytesReference readAllToReleasableBytesReference() throws IOException {
                return retainAndSkip(length() - offset());
            }

            @Override
            public boolean supportReadAllToReleasableBytesReference() {
                return true;
            }
        };
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        delegate.writeTo(os);
    }

    @Override
    public String utf8ToString() {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.utf8ToString();
    }

    @Override
    public BytesRef toBytesRef() {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.toBytesRef();
    }

    @Override
    public BytesRefIterator iterator() {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.iterator();
    }

    @Override
    public int compareTo(BytesReference o) {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.compareTo(o);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.toXContent(builder, params);
    }

    @Override
    public boolean isFragment() {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.isFragment();
    }

    @Override
    public boolean equals(Object obj) {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.equals(obj);
    }

    @Override
    public int hashCode() {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.hashCode();
    }

    @Override
    public String toString() {
        return getClass().getName() + "@" + getId();
    }

    @Override
    public boolean hasArray() {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.hasArray();
    }

    @Override
    public byte[] array() {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.array();
    }

    @Override
    public int arrayOffset() {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate.arrayOffset();
    }

    public BytesReference delegate() {
        assert hasReferences() : "used without holding reference source=[" + this + "]";
        return delegate;
    }

    private static final class RefCountedReleasable extends AbstractRefCounted {

        private final Releasable releasable;

        RefCountedReleasable(Releasable releasable) {
            this.releasable = releasable;
        }

        @Override
        protected void closeInternal() {
            Releasables.closeExpectNoException(releasable);
        }
    }
}
