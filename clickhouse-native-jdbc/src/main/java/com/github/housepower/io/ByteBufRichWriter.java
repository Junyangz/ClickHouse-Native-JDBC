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

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

public class ByteBufRichWriter implements RichWriter, ByteBufHelper, CodecHelper {

    private final ByteBuf buf;

    public ByteBufRichWriter(ByteBuf buf) {
        this.buf = buf;
    }

    public ByteBuf internalByteBuf() {
        return this.buf;
    }

    @Override
    public void writeBoolean(boolean b) {
        buf.writeBoolean(b);
    }

    @Override
    public void writeByte(byte b) {
        buf.writeByte(b);
    }

    @Override
    public void writeShortLE(short s) {
        buf.writeShortLE(s);
    }

    @Override
    public void writeIntLE(int i) {
        buf.writeIntLE(i);
    }

    @Override
    public void writeLongLE(long l) {
        buf.writeLongLE(l);
    }

    @Override
    public void writeVarInt(long v) {
        writeVarInt(buf, v);
    }

    @Override
    public void writeFloatLE(float f) {
        buf.writeFloatLE(f);
    }

    @Override
    public void writeDoubleLE(double d) {
        buf.writeDoubleLE(d);
    }

    @Override
    public void writeUTF8Binary(String utf8) {
        writeUTF8Binary(buf, utf8);
    }

    @Override
    public void writeStringBinary(String data, Charset charset) {
        writeCharSeqBinary(buf, data, charset);
    }

    @Override
    public void writeBytesBinary(byte[] bytes) {
        writeBinary(buf, bytes);
    }

    @Override
    public void writeBytes(byte[] bytes) {
        buf.writeBytes(bytes);
    }

    @Override
    public void flush(boolean force) {

    }
}
