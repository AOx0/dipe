#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![deny(rust_2018_idioms, unsafe_code)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![deny(clippy::unwrap_used)]

/// Iterate over all contiguous stings of alphabetical characters
pub fn get_words(cadena: &str) -> impl Iterator<Item = &str> {
    cadena
        .split(' ')
        .flat_map(|a| a.split('\n'))
        .flat_map(|a| a.split('\t'))
        .flat_map(|a| a.split('\r'))
        .map(str::trim)
        .filter(|a| !a.is_empty())
}

/// Iterator that yields a space between each item from the original iterator
pub fn space_join<'a>(mut iter: impl Iterator<Item = &'a str>) -> impl Iterator<Item = &'a str> {
    SpaceJoiner {
        next_is_space: true,
        next_str: iter.next(),
        inner: iter,
    }
}

/// Iterator that yields each word in a string with a space between each one
///
/// # Examples
///
/// ```
/// use strings::sanitize_spaces_iter;
///
/// let text = "\t\t\n Hello, \n\n\t \r\n world!\n\t\n";
///
/// assert_eq!(sanitize_spaces_iter(text).collect::<Vec<_>>(), vec!["Hello,", " ", "world!"]);
/// ```
pub fn sanitize_spaces_iter(string: &str) -> impl Iterator<Item = &str> {
    space_join(get_words(string))
}

/// Iterator that yields each word in a string with a space between each one
///
/// # Examples
///
/// ```
/// use strings::sanitize_spaces;
///
/// let text = "\t\t\n Hello,\n\n\t \r\n world!\n\t\n";
///
/// assert_eq!(sanitize_spaces(text), "Hello, world!".to_string());
/// ```
#[must_use]
pub fn sanitize_spaces(string: &str) -> String {
    let words = get_words(string);
    let joinner = space_join(words);

    joinner.collect::<String>()
}

impl<'a, T: Iterator<Item = &'a str>> Iterator for SpaceJoiner<'a, T> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_is_space = !self.next_is_space;
        if self.next_is_space && self.next_str.is_some() {
            Some(" ")
        } else if self.next_is_space {
            None
        } else {
            let a = self.next_str;
            self.next_str = self.inner.next();
            a
        }
    }
}

pub struct SpaceJoiner<'a, I>
where
    I: Iterator<Item = &'a str>,
{
    next_is_space: bool,
    next_str: Option<&'a str>,
    inner: I,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spaces_into_string() {
        let expect = ["hola", "como", "estas"];
        let inp = "\n\n\t\n   hola\t\t\n \n\t\n como\n\n\t\t\n\n\t \n \testas\n\n\t";

        let expect = expect.into_iter().collect::<Vec<_>>().join(" ");
        let got = space_join(get_words(inp)).collect::<String>();

        assert_eq!(expect, got);
    }

    #[test]
    fn spaces() {
        let expect = ["hola", "como", "estas"];
        let inp = "\n\n\t\n   hola\t\t\n \n\t\n como\n\n\t\t\n\n\t \n \testas\n\n\t";

        assert!(
            expect.into_iter().eq(get_words(inp)),
            "{:?} vs {:?}",
            expect.iter().collect::<Vec<_>>(),
            get_words(inp).collect::<Vec<_>>()
        );
    }
}
