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

import java.nio.charset.StandardCharsets;

public class ByteBufRichReader implements RichReader, ByteBufHelper, CodecHelper {

    private final ByteBuf buf;

    public ByteBufRichReader(ByteBuf buf) {
        this.buf = buf;
    }

    public ByteBuf internalByteBuf() {
        return this.buf;
    }

    @Override
    public boolean readBoolean() {
        return buf.readBoolean();
    }

    @Override
    public byte readByte() {
        return buf.readByte();
    }

    @Override
    public short readShortLE() {
        return buf.readShortLE();
    }

    @Override
    public int readIntLE() {
        return buf.readIntLE();
    }

    @Override
    public long readLongLE() {
        return buf.readLongLE();
    }

    @Override
    public long readVarInt() {
        return readVarInt(buf);
    }

    @Override
    public float readFloatLE() {
        return buf.readFloatLE();
    }

    @Override
    public double readDoubleLE() {
        return buf.readDoubleLE();
    }

    @Override
    public byte[] readBytes(int size) {
        byte[] data = new byte[size];
        buf.readBytes(data);
        return data;
    }

    @Override
    public byte[] readBytesBinary() {
        byte[] data = new byte[(int) readVarInt()];
        buf.readBytes(data);
        return data;
    }

    @Override
    public String readUTF8Binary() {
        byte[] data = readBytesBinary();
        return data.length > 0 ? new String(data, StandardCharsets.UTF_8) : "";
    }
}
