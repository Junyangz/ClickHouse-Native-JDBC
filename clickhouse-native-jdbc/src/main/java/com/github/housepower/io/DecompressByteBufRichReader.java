/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.housepower.io;

import io.airlift.compress.Decompressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import io.netty.buffer.ByteBuf;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;

import static com.github.housepower.settings.ClickHouseDefines.CHECKSUM_LENGTH;
import static com.github.housepower.settings.ClickHouseDefines.COMPRESSION_HEADER_LENGTH;

public class DecompressByteBufRichReader implements RichReader, ByteBufHelper, CodecHelper {

    private final ByteBuf compressedBuf;
    private final ByteBuf decompressedBuf;

    private final Decompressor lz4Decompressor = new Lz4Decompressor();
    private final Decompressor zstdDecompressor = new ZstdDecompressor();

    public DecompressByteBufRichReader(ByteBuf buf) {
        this.compressedBuf = buf;
        this.decompressedBuf = buf.alloc().buffer(); // TODO set a init size
    }

    @Override
    public boolean readBoolean() {
        maybeDecompress();
        return decompressedBuf.readBoolean();
    }

    @Override
    public byte readByte() {
        maybeDecompress();
        return decompressedBuf.readByte();
    }

    @Override
    public short readShortLE() {
        maybeDecompress();
        return decompressedBuf.readShortLE();
    }

    @Override
    public int readIntLE() {
        maybeDecompress();
        return decompressedBuf.readIntLE();
    }

    @Override
    public long readLongLE() {
        maybeDecompress();
        return decompressedBuf.readLongLE();
    }

    @Override
    public long readVarInt() {
        maybeDecompress();
        return readVarInt(decompressedBuf);
    }

    @Override
    public float readFloatLE() {
        maybeDecompress();
        return decompressedBuf.readFloatLE();
    }

    @Override
    public double readDoubleLE() {
        maybeDecompress();
        return decompressedBuf.readDoubleLE();
    }

    @Override
    public byte[] readBytes(int size) {
        maybeDecompress();
        byte[] data = new byte[size];
        decompressedBuf.readBytes(data);
        return data;
    }

    @Override
    public byte[] readBytesBinary() {
        maybeDecompress();
        byte[] data = new byte[(int) readVarInt()];
        decompressedBuf.readBytes(data);
        return data;
    }

    @Override
    public String readUTF8Binary() {
        byte[] data = readBytesBinary();
        return data.length > 0 ? new String(data, StandardCharsets.UTF_8) : "";
    }

    private void maybeDecompress() {
        if (decompressedBuf.isReadable())
            return;

        decompressedBuf.clear();

        compressedBuf.skipBytes(CHECKSUM_LENGTH); // TODO validate checksum
        int compressMethod = compressedBuf.readUnsignedByte() & 0x0FF;
        int compressedSize = compressedBuf.readIntLE();
        int decompressedSize = compressedBuf.readIntLE();
        switch (compressMethod) {
            case LZ4:
                readCompressedData(compressedSize - COMPRESSION_HEADER_LENGTH, decompressedSize, lz4Decompressor);
            case ZSTD:
                readCompressedData(compressedSize - COMPRESSION_HEADER_LENGTH, decompressedSize, zstdDecompressor);
            case NONE:
                readCompressedData(compressedSize - COMPRESSION_HEADER_LENGTH, decompressedSize, null);
            default:
                throw new UnsupportedOperationException("Unknown compression magic code: " + compressMethod);
        }
    }

    private void readCompressedData(int compressedSize, int decompressedSize, @Nullable Decompressor decompressor) {
        if (decompressor == null) {
            decompressedBuf.writeBytes(compressedBuf, compressedSize);
        } else {
            decompressor.decompress(
                    compressedBuf.slice(compressedBuf.readerIndex(), compressedSize).nioBuffer(),
                    decompressedBuf.slice(decompressedBuf.readerIndex(), decompressedSize).nioBuffer());
        }
    }
}
