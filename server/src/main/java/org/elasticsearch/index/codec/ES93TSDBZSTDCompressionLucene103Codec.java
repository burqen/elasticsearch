/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.bloomfilter.ES93BloomFilterStoredFieldsFormat;
import org.elasticsearch.index.codec.storedfields.TSDBStoredFieldsFormat;
import org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdCodec;
import org.elasticsearch.index.mapper.IdFieldMapper;

public class ES93TSDBZSTDCompressionLucene103Codec extends FilterCodec {
    private final TSDBStoredFieldsFormat storedFieldsFormat;

    /** Public no-arg constructor, needed for SPI loading at read-time. */
    public ES93TSDBZSTDCompressionLucene103Codec() {
        this(new Elasticsearch92Lucene103Codec(), null);
    }

    public ES93TSDBZSTDCompressionLucene103Codec(Elasticsearch92Lucene103Codec delegate, BigArrays bigArrays) {
        super("Elasticsearch93ZSTDStoredFieldsLucene103Codec", new TSDBSyntheticIdCodec(delegate));
        this.storedFieldsFormat = new TSDBStoredFieldsFormat(
            delegate.storedFieldsFormat(),
            new ES93BloomFilterStoredFieldsFormat(
                bigArrays,
                ES93BloomFilterStoredFieldsFormat.DEFAULT_BLOOM_FILTER_SIZE,
                IdFieldMapper.NAME
            )
        );
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }
}
