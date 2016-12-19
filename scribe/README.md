# Scribe

Scribe is a small, messaging oriented encoding library.  It was born out of 
indecision of whether I should use a human-readable encoding scheme or
a binary scheme.   Text is great in that humans can understand and interact
with them, but they are also verbose and less efficient.  Moreover, once tied 
to a particular encoding format, moving to another format can be difficult.  
So, scribe offers an intermediate data format that can act as a simple layer of
indirection.

# TODOS:

Rip out the type system and replace with trivial system.
