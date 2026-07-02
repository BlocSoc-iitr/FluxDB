use crate::TypeNameError;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::mem::size_of;

fn encode_usize_varint(mut value: usize, result: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            result.push(byte);
            break;
        }
        byte |= 0x80;
        result.push(byte);
    }
}

fn decode_usize_varint(data: &[u8], offset: &mut usize) -> usize {
    let mut result = 0usize;
    let mut shift = 0;

    loop {
        let byte = data[*offset];
        *offset += 1;
        result |= ((byte & 0x7f) as usize) << shift;

        if byte & 0x80 == 0 {
            return result;
        }

        shift += 7;
        assert!(
            shift < usize::BITS as usize,
            "varint is too large for usize"
        );
    }
}

#[derive(Eq, PartialEq, Debug, Clone)]
enum TypeClassification {
    Internal,
    UserDefined,
}

impl TypeClassification {
    fn to_byte(&self) -> u8 {
        match self {
            TypeClassification::Internal => 1,
            TypeClassification::UserDefined => 2,
        }
    }

    fn from_byte(value: u8) -> Result<Self, TypeNameError> {
        match value {
            1 => Ok(TypeClassification::Internal),
            2 => Ok(TypeClassification::UserDefined),
            v => Err(TypeNameError::UnknownClassification(v)),
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct TypeName {
    classification: TypeClassification,
    name: String,
}

impl TypeName {
    pub fn new(name: &str) -> Self {
        TypeName {
            classification: TypeClassification::UserDefined,
            name: name.to_string(),
        }
    }

    pub fn internal(name: &str) -> Self {
        Self {
            classification: TypeClassification::Internal,
            name: name.to_string(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.name.len() + 1);
        result.push(self.classification.to_byte());
        result.extend_from_slice(self.name.as_bytes());
        result
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TypeNameError> {
        if bytes.is_empty() {
            return Err(TypeNameError::Empty);
        }
        let classification = TypeClassification::from_byte(bytes[0])?;
        let name = std::str::from_utf8(&bytes[1..])?.to_string();
        Ok(Self {
            classification,
            name,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
pub trait Value: Debug {
    //`SelfType<'a>` must be the same type as Self with all lifetimes replaced with 'a
    ///deserialized representation
    type SelfType<'a>: Debug + 'a
    where
        Self: 'a;
    // serialized representation
    type AsBytes<'a>: AsRef<[u8]> + 'a
    where
        Self: 'a;

    // Width of a fixed type, or None for variable width
    // tells the compiler if type is fixed width, example: u32
    fn fixed_width() -> Option<usize>;

    /// Deserializes data
    /// Implementations may return a view over data, or an owned type
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a;

    // Serialize the value to a slice
    // b lifetime >= a lifetime
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b;

    /// Globally unique identifier for this type
    fn type_name() -> TypeName;
}

pub trait Key: Value {
    /// Compare data1 with data2
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering;
}

impl Value for () {
    type SelfType<'a>
        = ()
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(0)
    }

    #[allow(clippy::unused_unit, clippy::semicolon_if_nothing_returned)]
    fn from_bytes<'a>(_data: &'a [u8]) -> ()
    where
        Self: 'a,
    {
        ()
    }

    #[allow(clippy::ignored_unit_patterns)]
    fn as_bytes<'a, 'b: 'a>(_: &'a Self::SelfType<'b>) -> &'a [u8]
    where
        Self: 'b,
    {
        &[]
    }

    fn type_name() -> TypeName {
        TypeName::internal("()")
    }
}

impl Key for () {
    fn compare(_data1: &[u8], _data2: &[u8]) -> Ordering {
        Ordering::Equal
    }
}

impl Value for bool {
    type SelfType<'a>
        = bool
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(1)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> bool
    where
        Self: 'a,
    {
        match data[0] {
            0 => false,
            1 => true,
            _ => unreachable!(),
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a [u8]
    where
        Self: 'b,
    {
        match value {
            true => &[1],
            false => &[0],
        }
    }

    fn type_name() -> TypeName {
        TypeName::internal("bool")
    }
}

impl Key for bool {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let value1 = Self::from_bytes(data1);
        let value2 = Self::from_bytes(data2);
        value1.cmp(&value2)
    }
}

impl<T: Value> Value for Option<T> {
    type SelfType<'a>
        = Option<T::SelfType<'a>>
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        T::fixed_width().map(|width| width + 1)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Option<T::SelfType<'a>>
    where
        Self: 'a,
    {
        match data[0] {
            0 => None,
            1 => Some(T::from_bytes(&data[1..])),
            _ => unreachable!(),
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Vec<u8>
    where
        Self: 'b,
    {
        let mut result = vec![0];
        if let Some(inner) = value {
            result[0] = 1;
            result.extend_from_slice(T::as_bytes(inner).as_ref());
        } else if let Some(width) = T::fixed_width() {
            result.resize(width + 1, 0);
        }
        result
    }

    fn type_name() -> TypeName {
        TypeName::internal(&format!("Option<{}>", T::type_name().name()))
    }
}

impl<T: Key> Key for Option<T> {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        match (data1[0], data2[0]) {
            (0, 0) => Ordering::Equal,
            (0, 1) => Ordering::Less,
            (1, 0) => Ordering::Greater,
            (1, 1) => T::compare(&data1[1..], &data2[1..]),
            _ => unreachable!(),
        }
    }
}

impl Value for &[u8] {
    type SelfType<'a>
        = &'a [u8]
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> &'a [u8]
    where
        Self: 'a,
    {
        data
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a [u8]
    where
        Self: 'b,
    {
        value
    }

    fn type_name() -> TypeName {
        TypeName::internal("&[u8]")
    }
}

impl Key for &[u8] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

impl<const N: usize> Value for &[u8; N] {
    type SelfType<'a>
        = &'a [u8; N]
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8; N]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(N)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> &'a [u8; N]
    where
        Self: 'a,
    {
        data.try_into().unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a [u8; N]
    where
        Self: 'b,
    {
        value
    }

    fn type_name() -> TypeName {
        TypeName::internal(&format!("[u8;{N}]"))
    }
}

impl<const N: usize> Key for &[u8; N] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

impl<const N: usize, T: Value> Value for [T; N] {
    type SelfType<'a>
        = [T::SelfType<'a>; N]
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        T::fixed_width().map(|x| x * N)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> [T::SelfType<'a>; N]
    where
        Self: 'a,
    {
        let mut result = Vec::with_capacity(N);
        if let Some(fixed) = T::fixed_width() {
            for i in 0..N {
                result.push(T::from_bytes(&data[fixed * i..fixed * (i + 1)]));
            }
        } else {
            // Set offset to the first data item
            let mut start = size_of::<u32>() * N;
            for i in 0..N {
                let range = size_of::<u32>() * i..size_of::<u32>() * (i + 1);
                let end = u32::from_le_bytes(data[range].try_into().unwrap()) as usize;
                result.push(T::from_bytes(&data[start..end]));
                start = end;
            }
        }
        result.try_into().unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Vec<u8>
    where
        Self: 'b,
    {
        if let Some(fixed) = T::fixed_width() {
            let mut result = Vec::with_capacity(fixed * N);
            for item in value {
                result.extend_from_slice(T::as_bytes(item).as_ref());
            }
            result
        } else {
            // Reserve space for the end offsets
            // [offset1 offset2 offset3][data1][data2][data3]
            let mut result = vec![0u8; size_of::<u32>() * N];
            for i in 0..N {
                result.extend_from_slice(T::as_bytes(&value[i]).as_ref());
                let end: u32 = result.len().try_into().unwrap();
                result[size_of::<u32>() * i..size_of::<u32>() * (i + 1)]
                    .copy_from_slice(&end.to_le_bytes());
            }
            result
        }
    }

    fn type_name() -> TypeName {
        // Uses the same type name as [T;N] so that tables are compatible with [u8;N] and &[u8;N] types
        // This requires that the binary encoding be the same
        TypeName::internal(&format!("[{};{N}]", T::type_name().name()))
    }
}

impl<const N: usize, T: Key> Key for [T; N] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        if let Some(fixed) = T::fixed_width() {
            for i in 0..N {
                let range = fixed * i..fixed * (i + 1);
                let comparison = T::compare(&data1[range.clone()], &data2[range]);
                if !comparison.is_eq() {
                    return comparison;
                }
            }
        } else {
            // Set offset to the first data item
            let mut start1 = size_of::<u32>() * N;
            let mut start2 = size_of::<u32>() * N;
            for i in 0..N {
                let range = size_of::<u32>() * i..size_of::<u32>() * (i + 1);
                let end1 = u32::from_le_bytes(data1[range.clone()].try_into().unwrap()) as usize;
                let end2 = u32::from_le_bytes(data2[range].try_into().unwrap()) as usize;
                let comparison = T::compare(&data1[start1..end1], &data2[start2..end2]);
                if !comparison.is_eq() {
                    return comparison;
                }
                start1 = end1;
                start2 = end2;
            }
        }
        Ordering::Equal
    }
}

impl Value for &str {
    type SelfType<'a>
        = &'a str
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a str
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> &'a str
    where
        Self: 'a,
    {
        std::str::from_utf8(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a str
    where
        Self: 'b,
    {
        value
    }

    fn type_name() -> TypeName {
        TypeName::internal("&str")
    }
}

impl Key for &str {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        // UTF-8 sorts byte-lexicographically (str::cmp is defined as byte order),
        // so a raw slice compare matches `from_utf8(..).cmp(..)` without the
        // per-comparison UTF-8 revalidation of already-valid stored bytes.
        data1.cmp(data2)
    }
}

impl Value for String {
    type SelfType<'a>
        = String
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a str
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> String
    where
        Self: 'a,
    {
        std::str::from_utf8(data).unwrap().to_string()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a str
    where
        Self: 'b,
    {
        value.as_str()
    }

    fn type_name() -> TypeName {
        TypeName::internal("String")
    }
}

impl Key for String {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        // UTF-8 sorts byte-lexicographically (str::cmp is defined as byte order),
        // so a raw slice compare matches `from_utf8(..).cmp(..)` without the
        // per-comparison UTF-8 revalidation of already-valid stored bytes.
        data1.cmp(data2)
    }
}

impl Value for char {
    type SelfType<'a>
        = char
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; 3]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(3)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> char
    where
        Self: 'a,
    {
        char::from_u32(u32::from_le_bytes([data[0], data[1], data[2], 0])).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> [u8; 3]
    where
        Self: 'b,
    {
        let bytes = u32::from(*value).to_le_bytes();
        [bytes[0], bytes[1], bytes[2]]
    }

    fn type_name() -> TypeName {
        TypeName::internal("char")
    }
}

impl Key for char {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
    }
}

macro_rules! le_value {
    ($t:ty) => {
        impl Value for $t {
            type SelfType<'a>
                = $t
            where
                Self: 'a;
            type AsBytes<'a>
                = [u8; std::mem::size_of::<$t>()]
            where
                Self: 'a;

            fn fixed_width() -> Option<usize> {
                Some(std::mem::size_of::<$t>())
            }

            fn from_bytes<'a>(data: &'a [u8]) -> $t
            where
                Self: 'a,
            {
                <$t>::from_le_bytes(data.try_into().unwrap())
            }

            fn as_bytes<'a, 'b: 'a>(
                value: &'a Self::SelfType<'b>,
            ) -> [u8; std::mem::size_of::<$t>()]
            where
                Self: 'a,
                Self: 'b,
            {
                value.to_le_bytes()
            }

            fn type_name() -> TypeName {
                TypeName::internal(stringify!($t))
            }
        }
    };
}

macro_rules! le_key {
    ($t:ty) => {
        le_value!($t);

        impl Key for $t {
            fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
                Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
            }
        }
    };
}

le_key!(u8);
le_key!(u16);
le_key!(u32);
le_key!(u64);
le_key!(u128);
le_key!(i8);
le_key!(i16);
le_key!(i32);
le_key!(i64);
le_key!(i128);
le_value!(f32);
le_value!(f64);

impl<T: Value> Value for Vec<T> {
    type SelfType<'a>
        = Vec<T::SelfType<'a>>
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Vec<T::SelfType<'a>>
    where
        Self: 'a,
    {
        let mut offset = 0;
        let count = decode_usize_varint(data, &mut offset);
        let mut result = Vec::with_capacity(count);

        if let Some(fixed) = T::fixed_width() {
            for i in 0..count {
                let item_start = offset + fixed * i;
                let item_end = item_start + fixed;
                result.push(T::from_bytes(&data[item_start..item_end]));
            }
        } else {
            for _ in 0..count {
                let item_len = decode_usize_varint(data, &mut offset);
                let item_end = offset + item_len;
                result.push(T::from_bytes(&data[offset..item_end]));
                offset = item_end;
            }
        }

        result
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Vec<u8>
    where
        Self: 'b,
    {
        let mut result = Vec::new();
        encode_usize_varint(value.len(), &mut result);

        if let Some(fixed) = T::fixed_width() {
            result.reserve(fixed * value.len());
            for item in value {
                result.extend_from_slice(T::as_bytes(item).as_ref());
            }
        } else {
            for item in value {
                let item_bytes = T::as_bytes(item);
                encode_usize_varint(item_bytes.as_ref().len(), &mut result);
                result.extend_from_slice(item_bytes.as_ref());
            }
        }

        result
    }

    fn type_name() -> TypeName {
        TypeName::internal(&format!("Vec<{}>", T::type_name().name()))
    }
}

impl<T: Key> Key for Vec<T> {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        // Lexicographic: compare element-by-element, shorter-as-prefix is Less.
        // The byte layout is not order-preserving, so decode rather than memcmp.
        let mut offset1 = 0;
        let mut offset2 = 0;
        let count1 = decode_usize_varint(data1, &mut offset1);
        let count2 = decode_usize_varint(data2, &mut offset2);
        let common = count1.min(count2);

        if let Some(fixed) = T::fixed_width() {
            for _ in 0..common {
                let end1 = offset1 + fixed;
                let end2 = offset2 + fixed;
                let comparison = T::compare(&data1[offset1..end1], &data2[offset2..end2]);
                if !comparison.is_eq() {
                    return comparison;
                }
                offset1 = end1;
                offset2 = end2;
            }
        } else {
            for _ in 0..common {
                let len1 = decode_usize_varint(data1, &mut offset1);
                let len2 = decode_usize_varint(data2, &mut offset2);
                let end1 = offset1 + len1;
                let end2 = offset2 + len2;
                let comparison = T::compare(&data1[offset1..end1], &data2[offset2..end2]);
                if !comparison.is_eq() {
                    return comparison;
                }
                offset1 = end1;
                offset2 = end2;
            }
        }

        count1.cmp(&count2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_value<T: Value>() {}

    fn assert_key<T: Key>() {}

    #[test]
    fn primitive_values_roundtrip() {
        let bytes = <u8 as Value>::as_bytes(&42);
        assert_eq!(<u8 as Value>::from_bytes(bytes.as_ref()), 42);
        let bytes = <u16 as Value>::as_bytes(&513);
        assert_eq!(<u16 as Value>::from_bytes(bytes.as_ref()), 513);
        let bytes = <u32 as Value>::as_bytes(&123_456);
        assert_eq!(<u32 as Value>::from_bytes(bytes.as_ref()), 123_456);
        let bytes = <u64 as Value>::as_bytes(&123_456_789);
        assert_eq!(<u64 as Value>::from_bytes(bytes.as_ref()), 123_456_789);
        let bytes = <u128 as Value>::as_bytes(&123_456_789_123);
        assert_eq!(<u128 as Value>::from_bytes(bytes.as_ref()), 123_456_789_123);
        let bytes = <i8 as Value>::as_bytes(&-42);
        assert_eq!(<i8 as Value>::from_bytes(bytes.as_ref()), -42);
        let bytes = <i16 as Value>::as_bytes(&-513);
        assert_eq!(<i16 as Value>::from_bytes(bytes.as_ref()), -513);
        let bytes = <i32 as Value>::as_bytes(&-123_456);
        assert_eq!(<i32 as Value>::from_bytes(bytes.as_ref()), -123_456);
        let bytes = <i64 as Value>::as_bytes(&-123_456_789);
        assert_eq!(<i64 as Value>::from_bytes(bytes.as_ref()), -123_456_789);
        let bytes = <i128 as Value>::as_bytes(&-123_456_789_123);
        assert_eq!(
            <i128 as Value>::from_bytes(bytes.as_ref()),
            -123_456_789_123
        );
        let bytes = <f32 as Value>::as_bytes(&3.5);
        assert_eq!(<f32 as Value>::from_bytes(bytes.as_ref()), 3.5);
        let bytes = <f64 as Value>::as_bytes(&-9.25);
        assert_eq!(<f64 as Value>::from_bytes(bytes.as_ref()), -9.25);
    }

    #[test]
    fn string_and_char_values_roundtrip() {
        let bytes = <&str as Value>::as_bytes(&"flux");
        assert_eq!(<&str as Value>::from_bytes(bytes.as_ref()), "flux");
        let value = "database".to_string();
        let bytes = <String as Value>::as_bytes(&value);
        assert_eq!(<String as Value>::from_bytes(bytes.as_ref()), "database");
        let bytes = <char as Value>::as_bytes(&'λ');
        assert_eq!(<char as Value>::from_bytes(bytes.as_ref()), 'λ');
        let bytes = <char as Value>::as_bytes(&'\u{10ffff}');
        assert_eq!(<char as Value>::from_bytes(bytes.as_ref()), '\u{10ffff}');
    }

    #[test]
    fn option_values_roundtrip() {
        let some = Some(42u32);
        let none: Option<u32> = None;

        let bytes = <Option<u32> as Value>::as_bytes(&some);
        assert_eq!(<Option<u32> as Value>::from_bytes(bytes.as_ref()), Some(42));
        let bytes = <Option<u32> as Value>::as_bytes(&none);
        assert_eq!(<Option<u32> as Value>::from_bytes(bytes.as_ref()), None);
        assert_eq!(<Option<u32> as Value>::fixed_width(), Some(5));
        assert_eq!(<Option<&str> as Value>::fixed_width(), None);
    }

    #[test]
    fn option_variable_width_roundtrip() {
        let some = Some("hello");
        let none: Option<&str> = None;

        let bytes = <Option<&str> as Value>::as_bytes(&some);
        assert_eq!(<Option<&str> as Value>::from_bytes(bytes.as_ref()), some);

        let bytes = <Option<&str> as Value>::as_bytes(&none);
        assert_eq!(<Option<&str> as Value>::from_bytes(bytes.as_ref()), none);
    }

    #[test]
    fn vec_values_roundtrip() {
        let fixed = vec![1u32, 2, 3, 4];
        let variable = vec!["alpha", "beta", "gamma"];
        let empty: Vec<u64> = Vec::new();

        let bytes = <Vec<u32> as Value>::as_bytes(&fixed);
        assert_eq!(<Vec<u32> as Value>::from_bytes(bytes.as_ref()), fixed);
        let bytes = <Vec<&str> as Value>::as_bytes(&variable);
        assert_eq!(bytes.as_slice(), b"\x03\x05alpha\x04beta\x05gamma");
        assert_eq!(<Vec<&str> as Value>::from_bytes(bytes.as_ref()), variable);
        let bytes = <Vec<u64> as Value>::as_bytes(&empty);
        assert_eq!(<Vec<u64> as Value>::from_bytes(bytes.as_ref()), empty);
    }

    #[test]
    fn arrays_and_fixed_byte_refs_roundtrip() {
        let array = [1u32, 2, 3];
        let byte_array = *b"flux";
        let byte_array_ref = &byte_array;

        let bytes = <[u32; 3] as Value>::as_bytes(&array);
        assert_eq!(<[u32; 3] as Value>::from_bytes(bytes.as_ref()), array);
        let bytes = <&[u8; 4] as Value>::as_bytes(&byte_array_ref);
        assert_eq!(
            <&[u8; 4] as Value>::from_bytes(bytes.as_ref()),
            byte_array_ref
        );
        assert_eq!(
            <&[u8; 4] as Value>::type_name(),
            <[u8; 4] as Value>::type_name()
        );
    }

    #[test]
    fn key_ordering_uses_logical_order() {
        assert_eq!(
            u32::compare(&10u32.to_le_bytes(), &2u32.to_le_bytes()),
            Ordering::Greater
        );
        assert_eq!(
            i32::compare(&(-10i32).to_le_bytes(), &2i32.to_le_bytes()),
            Ordering::Less
        );
        assert_eq!(<&str>::compare(b"alpha", b"beta"), Ordering::Less);

        let lambda = <char as Value>::as_bytes(&'λ');
        let omega = <char as Value>::as_bytes(&'ω');
        assert_eq!(char::compare(&lambda, &omega), Ordering::Less);

        let none = <Option<u32> as Value>::as_bytes(&None);
        let some_1 = <Option<u32> as Value>::as_bytes(&Some(1));
        let some_2 = <Option<u32> as Value>::as_bytes(&Some(2));
        assert_eq!(
            <Option<u32> as Key>::compare(&none, &some_1),
            Ordering::Less
        );
        assert_eq!(
            <Option<u32> as Key>::compare(&some_1, &some_2),
            Ordering::Less
        );

        assert_eq!(<&[u8; 3] as Key>::compare(b"abc", b"abd"), Ordering::Less);
    }

    #[test]
    fn representative_types_satisfy_trait_bounds() {
        assert_value::<u64>();
        assert_value::<i64>();
        assert_value::<f32>();
        assert_value::<f64>();
        assert_value::<&str>();
        assert_value::<String>();
        assert_value::<Option<&str>>();
        assert_value::<Vec<u32>>();
        assert_value::<Vec<&str>>();
        assert_value::<&[u8; 8]>();

        assert_key::<u64>();
        assert_key::<i64>();
        assert_key::<&str>();
        assert_key::<String>();
        assert_key::<Option<&str>>();
        assert_key::<&[u8; 8]>();
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    // from_bytes(as_bytes(x)) == x for owned-SelfType types.
    // Restricted to types whose SelfType<'a> is the type itself (the owned set:
    // scalars, Option<scalar>, Vec<scalar>) so the decoded value borrows nothing.
    fn check_roundtrip<T>(value: T) -> Result<(), TestCaseError>
    where
        T: Value + PartialEq + Debug,
        for<'a> T: Value<SelfType<'a> = T>,
    {
        // Scope `bytes` so its borrow of `value` ends before the assert moves `value`.
        let decoded = {
            let bytes = T::as_bytes(&value);
            T::from_bytes(bytes.as_ref())
        };
        prop_assert_eq!(decoded, value);
        Ok(())
    }

    fn check_roundtrip_ref<T>(view: T::SelfType<'_>) -> Result<(), TestCaseError>
    where
        T: Value,
        for<'a> T::SelfType<'a>: Debug,
        for<'a, 'b> T::SelfType<'a>: PartialEq<T::SelfType<'b>>,
    {
        let bytes = T::as_bytes(&view);
        let decoded = T::from_bytes(bytes.as_ref());
        prop_assert!(
            decoded == view,
            "roundtrip mismatch: {:?} != {:?}",
            decoded,
            view
        );
        Ok(())
    }

    // K::compare(as_bytes(a), as_bytes(b)) == a.cmp(&b) for owned-SelfType keys.
    // The byte-level compare must agree with the type's native Ord.
    fn check_order<T>(a: T, b: T) -> Result<(), TestCaseError>
    where
        T: Key + Ord + Debug,
        for<'a> T: Value<SelfType<'a> = T>,
    {
        let expected = a.cmp(&b);
        let actual = {
            let bytes_a = T::as_bytes(&a);
            let bytes_b = T::as_bytes(&b);
            T::compare(bytes_a.as_ref(), bytes_b.as_ref())
        };
        prop_assert_eq!(actual, expected);
        Ok(())
    }

    proptest! {
        // --- Group A: owned-SelfType types (same helper as the integer scalars) ---
        #[test]
        fn bool_roundtrip(x in any::<bool>()) { check_roundtrip::<bool>(x)?; }
        #[test]
        fn char_roundtrip(x in any::<char>()) { check_roundtrip::<char>(x)?; }
        #[test]
        fn unit_roundtrip(x in any::<()>()) { check_roundtrip::<()>(x)?; }
        #[test]
        fn string_roundtrip(x in any::<String>()) { check_roundtrip::<String>(x)?; }
        #[test]
        fn array_u32_roundtrip(x in any::<[u32; 3]>()) { check_roundtrip::<[u32; 3]>(x)?; }
        #[test]
        fn option_i64_roundtrip(x in any::<Option<i64>>()) { check_roundtrip::<Option<i64>>(x)?; }
        #[test]
        fn vec_i64_roundtrip(x in any::<Vec<i64>>()) { check_roundtrip::<Vec<i64>>(x)?; }

        // --- Group B: borrowed-SelfType types (own the data, pass a view) ---
        #[test]
        fn str_roundtrip(s in any::<String>()) {
            check_roundtrip_ref::<&str>(s.as_str())?;
        }
        #[test]
        fn bytes_roundtrip(v in any::<Vec<u8>>()) {
            check_roundtrip_ref::<&[u8]>(v.as_slice())?;
        }
        #[test]
        fn byte_array_ref_roundtrip(a in any::<[u8; 8]>()) {
            check_roundtrip_ref::<&[u8; 8]>(&a)?;
        }
        // Option has no cross-lifetime PartialEq impl, so the generic ref helper can't
        // type it; written concretely instead, where variance lets the lifetimes unify.
        #[test]
        fn option_str_roundtrip(s in any::<Option<String>>()) {
            let view: Option<&str> = s.as_deref();
            let bytes = <Option<&str> as Value>::as_bytes(&view);
            let decoded = <Option<&str> as Value>::from_bytes(bytes.as_ref());
            prop_assert_eq!(decoded, view);
        }
        #[test]
        fn vec_str_roundtrip(strings in any::<Vec<String>>()) {
            let views: Vec<&str> = strings.iter().map(String::as_str).collect();
            check_roundtrip_ref::<Vec<&str>>(views)?;
        }

        #[test]
        fn u8_roundtrip(x in any::<u8>()) { check_roundtrip::<u8>(x)?; }
        #[test]
        fn u16_roundtrip(x in any::<u16>()) { check_roundtrip::<u16>(x)?; }
        #[test]
        fn u32_roundtrip(x in any::<u32>()) { check_roundtrip::<u32>(x)?; }
        #[test]
        fn u64_roundtrip(x in any::<u64>()) { check_roundtrip::<u64>(x)?; }
        #[test]
        fn u128_roundtrip(x in any::<u128>()) { check_roundtrip::<u128>(x)?; }
        #[test]
        fn i8_roundtrip(x in any::<i8>()) { check_roundtrip::<i8>(x)?; }
        #[test]
        fn i16_roundtrip(x in any::<i16>()) { check_roundtrip::<i16>(x)?; }
        #[test]
        fn i32_roundtrip(x in any::<i32>()) { check_roundtrip::<i32>(x)?; }
        #[test]
        fn i64_roundtrip(x in any::<i64>()) { check_roundtrip::<i64>(x)?; }
        #[test]
        fn i128_roundtrip(x in any::<i128>()) { check_roundtrip::<i128>(x)?; }
        #[test]
        fn option_u32_roundtrip(x in any::<Option<u32>>()) {
            check_roundtrip::<Option<u32>>(x)?;
        }
        #[test]
        fn vec_u32_roundtrip(x in any::<Vec<u32>>()) {
            check_roundtrip::<Vec<u32>>(x)?;
        }

        // --- Order-preserving: K::compare agrees with native Ord ---
        // i64: regression-lock (compare = deserialize + native cmp).
        #[test]
        fn i64_order(a in any::<i64>(), b in any::<i64>()) {
            check_order::<i64>(a, b)?;
        }
        // Option<u32>: exercises the None/Some discriminant branches.
        #[test]
        fn option_u32_order(a in any::<Option<u32>>(), b in any::<Option<u32>>()) {
            check_order::<Option<u32>>(a, b)?;
        }
        // [u32; 3]: fixed-width array short-circuit. Small element domain so the
        // arrays frequently share a prefix and differ at element 1 or 2.
        #[test]
        fn array_u32_order(
            a in prop::array::uniform3(0u32..4),
            b in prop::array::uniform3(0u32..4),
        ) {
            check_order::<[u32; 3]>(a, b)?;
        }
        // Vec<u32>: fixed-width Vec compare + length tiebreak ([1] < [1, 2]).
        #[test]
        fn vec_u32_order(
            a in prop::collection::vec(0u32..4, 0..6),
            b in prop::collection::vec(0u32..4, 0..6),
        ) {
            check_order::<Vec<u32>>(a, b)?;
        }
        // Vec<String>: variable-width Vec compare (per-element varints) + tiebreak.
        #[test]
        fn vec_string_order(
            a in prop::collection::vec("[a-c]{0,3}", 0..5),
            b in prop::collection::vec("[a-c]{0,3}", 0..5),
        ) {
            check_order::<Vec<String>>(a, b)?;
        }
    }
}
