#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![deny(rust_2018_idioms, unsafe_code)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![deny(clippy::unwrap_used)]

use unicode_normalization::UnicodeNormalization;

pub use edit_distance::edit_distance;

/// Iterate over all contiguous stings of alphabetical characters
pub fn get_words(cadena: &str) -> impl Iterator<Item = &str> {
    get_words_ext(cadena, &[])
}

pub fn sanitize<'a>(contents: &'a str, seps: &'a [char]) -> impl Iterator<Item = char> + 'a {
    chars_to_lower(rm_specials(space_join(get_words_ext(contents, seps))))
}

pub fn get_words_ext<'a>(cadena: &'a str, extras: &'a [char]) -> impl Iterator<Item = &'a str> {
    cadena
        .split(' ')
        .flat_map(|a| a.split(&['\n', '\t', '\r']))
        .flat_map(move |a| a.split(extras))
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

pub fn replace_chars<'a>(
    chars: impl Iterator<Item = char> + 'a,
    matches: &'a [char],
    to: char,
) -> impl Iterator<Item = char> + 'a {
    chars.map(move |c| if matches.contains(&c) { to } else { c })
}

/// Compare the first letter of each word
#[must_use]
pub fn compare_words(slice: &str, with: &str) -> bool {
    let words1 = get_words(slice);
    let words2 = get_words(with);

    let first_chars1 = n_chars(words1, 1);
    let first_chars2 = n_chars(words2, 1);

    first_chars1.eq(first_chars2)
}

pub fn rm_specials<'a>(
    word: impl Iterator<Item = &'a str> + 'a,
) -> impl Iterator<Item = char> + 'a {
    word.flat_map(move |w| {
        w.chars()
            .map(move |c| c.nfd().next().expect("All chars have at least one char?"))
    })
}

pub fn rm_specials_char<'a>(
    word: impl Iterator<Item = char> + 'a,
) -> impl Iterator<Item = char> + 'a {
    word.map(move |c| c.nfd().next().expect("All chars have at least one char?"))
}

pub fn chars_to_lower<'a>(
    chars: impl Iterator<Item = char> + 'a,
) -> impl Iterator<Item = char> + 'a {
    chars.map(|c| c.to_ascii_lowercase())
}

pub fn chars_to_upper<'a>(
    chars: impl Iterator<Item = char> + 'a,
) -> impl Iterator<Item = char> + 'a {
    chars.map(|c| c.to_ascii_uppercase())
}

pub fn n_chars<'a>(
    word: impl Iterator<Item = &'a str> + 'a,
    n: usize,
) -> impl Iterator<Item = char> + 'a {
    word.flat_map(move |w| w.chars().take(n).filter_map(|c| c.to_lowercase().next()))
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

    #[test]
    fn take_chars_2() {
        let str = "Hola me llamo daniel";
        let expected = &['h', 'o', 'm', 'e', 'l', 'l', 'd', 'a'];

        let res = n_chars(get_words(str), 2).collect::<Vec<_>>();

        assert_eq!(res.as_slice(), expected.as_slice());
    }

    #[test]
    fn take_chars_spaces_2() {
        let str = "Hola me llamo Daniel";
        let expected = &['h', 'o', ' ', 'm', 'e', ' ', 'l', 'l', ' ', 'd', 'a'];

        let res = n_chars(space_join(get_words(str)), 2).collect::<Vec<_>>();

        assert_eq!(res.as_slice(), expected.as_slice());
    }

    #[test]
    fn normalize_char() {
        let str = "Eyyyy cómo andamos mi Pepe perro ajá";
        let expected = [
            'E', 'y', 'y', 'y', 'y', ' ', 'c', 'o', 'm', 'o', ' ', 'a', 'n', 'd', 'a', 'm', 'o',
            's', ' ', 'm', 'i', ' ', 'P', 'e', 'p', 'e', ' ', 'p', 'e', 'r', 'r', 'o', ' ', 'a',
            'j', 'a',
        ];

        let res = rm_specials(space_join(get_words(str))).collect::<Vec<_>>();
        assert_eq!(res.as_slice(), expected.as_slice());
    }

    #[test]
    fn compare() {
        let name1 = "Juan P. Rodriguez Pérez";
        let name2 = "Juan Pablo R. P.";

        assert!(compare_words(name1, name2));
    }
}
