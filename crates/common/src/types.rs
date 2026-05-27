use crate::TypeNameError;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::mem::size_of;

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
