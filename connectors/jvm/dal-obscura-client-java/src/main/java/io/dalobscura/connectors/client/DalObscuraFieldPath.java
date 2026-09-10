package io.dalobscura.connectors.client;

import io.dalobscura.flight.v1.DalObscuraFlightProto;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

/** Immutable version-one canonical path for governed nested Arrow fields. */
public final class DalObscuraFieldPath {
    public static final int VERSION = 1;

    private final List<Segment> segments;

    private DalObscuraFieldPath(List<Segment> segments) {
        if (segments.isEmpty() || segments.get(0).kind != Kind.FIELD) {
            throw new IllegalArgumentException("Field paths must start with a field segment");
        }
        this.segments = List.copyOf(segments);
    }

    public static DalObscuraFieldPath parse(String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Field path must be non-empty text");
        }
        List<Segment> parsed = new ArrayList<>();
        int offset = 0;
        while (offset < value.length()) {
            ParseToken token = value.charAt(offset) == '['
                    ? parseQuoted(value, offset)
                    : parseBare(value, offset);
            parsed.add(segmentFor(token.value, token.quoted));
            offset = token.next;
            if (offset == value.length()) {
                break;
            }
            if (value.charAt(offset) != '.') {
                throw new IllegalArgumentException("Expected '.' between field path segments");
            }
            offset++;
            if (offset == value.length()) {
                throw new IllegalArgumentException("Field path cannot end with '.'");
            }
        }
        return new DalObscuraFieldPath(parsed);
    }

    public static DalObscuraFieldPath of(List<Segment> segments) {
        return new DalObscuraFieldPath(segments);
    }

    public static Segment field(String name) {
        return field(name, OptionalInt.empty());
    }

    public static Segment field(String name, OptionalInt fieldId) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Field segment names must be non-empty");
        }
        return new Segment(Kind.FIELD, name, fieldId);
    }

    public static Segment listElement() {
        return new Segment(Kind.LIST_ELEMENT, "", OptionalInt.empty());
    }

    public static Segment mapKey() {
        return new Segment(Kind.MAP_KEY, "", OptionalInt.empty());
    }

    public static Segment mapValue() {
        return new Segment(Kind.MAP_VALUE, "", OptionalInt.empty());
    }

    public List<Segment> segments() {
        return segments;
    }

    DalObscuraFlightProto.FieldPath toProto() {
        DalObscuraFlightProto.FieldPath.Builder path =
                DalObscuraFlightProto.FieldPath.newBuilder().setVersion(VERSION);
        for (Segment segment : segments) {
            DalObscuraFlightProto.FieldPathSegment.Builder encoded =
                    DalObscuraFlightProto.FieldPathSegment.newBuilder().setKind(segment.kind.protoKind);
            if (segment.kind == Kind.FIELD) {
                encoded.setName(segment.name);
                if (segment.fieldId.isPresent()) {
                    encoded.setFieldId(segment.fieldId.getAsInt());
                }
            }
            path.addSegments(encoded);
        }
        return path.build();
    }

    public static final class Segment {
        private final Kind kind;
        private final String name;
        private final OptionalInt fieldId;

        private Segment(Kind kind, String name, OptionalInt fieldId) {
            this.kind = Objects.requireNonNull(kind, "kind");
            this.name = Objects.requireNonNull(name, "name");
            this.fieldId = Objects.requireNonNull(fieldId, "fieldId");
        }
    }

    private enum Kind {
        FIELD(DalObscuraFlightProto.FieldPathSegment.Kind.FIELD),
        LIST_ELEMENT(DalObscuraFlightProto.FieldPathSegment.Kind.LIST_ELEMENT),
        MAP_KEY(DalObscuraFlightProto.FieldPathSegment.Kind.MAP_KEY),
        MAP_VALUE(DalObscuraFlightProto.FieldPathSegment.Kind.MAP_VALUE);

        private final DalObscuraFlightProto.FieldPathSegment.Kind protoKind;

        Kind(DalObscuraFlightProto.FieldPathSegment.Kind protoKind) {
            this.protoKind = protoKind;
        }
    }

    private static Segment segmentFor(String token, boolean quoted) {
        if (quoted) {
            return field(token);
        }
        if ("$element".equals(token)) {
            return listElement();
        }
        if ("$key".equals(token)) {
            return mapKey();
        }
        if ("$value".equals(token)) {
            return mapValue();
        }
        if (token.startsWith("$")) {
            throw new IllegalArgumentException("Invalid collection path segment: " + token);
        }
        if (!token.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("Invalid field path segment: " + token);
        }
        return field(token);
    }

    private static ParseToken parseBare(String value, int offset) {
        int end = value.indexOf('.', offset);
        if (end < 0) {
            end = value.length();
        }
        String token = value.substring(offset, end);
        if (token.isEmpty()) {
            throw new IllegalArgumentException("Invalid field path segment");
        }
        return new ParseToken(token, end, false);
    }

    private static ParseToken parseQuoted(String value, int offset) {
        if (offset + 1 >= value.length() || value.charAt(offset + 1) != '"') {
            throw new IllegalArgumentException("Quoted field names must use JSON string syntax");
        }
        StringBuilder decoded = new StringBuilder();
        int index = offset + 2;
        while (index < value.length()) {
            char current = value.charAt(index++);
            if (current == '"') {
                if (index >= value.length() || value.charAt(index) != ']') {
                    throw new IllegalArgumentException("Quoted field names must use JSON string syntax");
                }
                if (decoded.length() == 0) {
                    throw new IllegalArgumentException("Quoted field names must be non-empty text");
                }
                return new ParseToken(decoded.toString(), index + 1, true);
            }
            if (current != '\\') {
                decoded.append(current);
                continue;
            }
            if (index >= value.length()) {
                break;
            }
            char escaped = value.charAt(index++);
            switch (escaped) {
                case '"': decoded.append('"'); break;
                case '\\': decoded.append('\\'); break;
                case '/': decoded.append('/'); break;
                case 'b': decoded.append('\b'); break;
                case 'f': decoded.append('\f'); break;
                case 'n': decoded.append('\n'); break;
                case 'r': decoded.append('\r'); break;
                case 't': decoded.append('\t'); break;
                case 'u':
                    if (index + 4 > value.length()) {
                        throw new IllegalArgumentException("Quoted field names must use JSON string syntax");
                    }
                    try {
                        decoded.append((char) Integer.parseInt(value.substring(index, index + 4), 16));
                    } catch (NumberFormatException error) {
                        throw new IllegalArgumentException("Quoted field names must use JSON string syntax", error);
                    }
                    index += 4;
                    break;
                default:
                    throw new IllegalArgumentException("Quoted field names must use JSON string syntax");
            }
        }
        throw new IllegalArgumentException("Unterminated quoted field name");
    }

    private static final class ParseToken {
        private final String value;
        private final int next;
        private final boolean quoted;

        private ParseToken(String value, int next, boolean quoted) {
            this.value = value;
            this.next = next;
            this.quoted = quoted;
        }
    }
}
