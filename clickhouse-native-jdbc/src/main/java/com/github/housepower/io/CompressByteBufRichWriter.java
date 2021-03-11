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

import io.airlift.compress.Compressor;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

public class CompressByteBufRichWriter implements RichWriter, ByteBufHelper, CodecHelper {

    private final int capacity;
    private final ByteBuf output;
    private final Compressor compressor;

    public CompressByteBufRichWriter(int capacity, ByteBufRichWriter writer, Compressor compressor) {
        this.capacity = capacity;
        this.output = writer.internalByteBuf();
        this.compressor = compressor;
    }

    private void maybeCompress() {

    }

    @Override
    public void writeBoolean(boolean b) {

    }

    @Override
    public void writeByte(byte b) {

    }

    @Override
    public void writeShortLE(short s) {

    }

    @Override
    public void writeIntLE(int i) {

    }

    @Override
    public void writeLongLE(long l) {

    }

    @Override
    public void writeVarInt(long v) {

    }

    @Override
    public void writeFloatLE(float f) {

    }

    @Override
    public void writeDoubleLE(double d) {

    }

    @Override
    public void writeBytes(byte[] bytes) {

    }

    @Override
    public void writeUTF8Binary(String utf8) {

    }

    @Override
    public void writeStringBinary(String seq, Charset charset) {

    }

    @Override
    public void writeBytesBinary(byte[] bytes) {

    }

    @Override
    public void flush(boolean force) {

    }
}
