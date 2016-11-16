# Scribe

Scribe is a small, messaging oriented encoding library.  It was born out of 
indecision of whether I should use a human-readable encoding scheme or
a binary scheme.   Text is great in that humans can understand and interact
with them, but they are also verbose and less efficient.  Moreover, once tied 
to a particular encoding format, moving to another format can be difficult.  
So, scribe offers an intermediate data format that can be 

## Message Oriented vs. Stream Oriented

I believe that one of the fundamental problems with existing encoding libraries
is the lack of distinction between a message and a stream.  For those unfamiliar
with these terms, the difference is identical to the difference between UDP
and TCP.  UDP is a message oriented protocol while TCP is stream oriented.
The key difference is that a message has a distinct beginning AND end, while
a stream does not.
