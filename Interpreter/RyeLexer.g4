lexer grammar RyeLexer;

// -- Reductions -- //
SET_REDUCTIONS 
	: A V G 
	| C O R R 
	| C O U N T 
	| C O U N T '_' A L L 
	| C O U N T '_' N U L L 
	| C O V A R 
	| F R E Q 
	| I N T E R C E P T 
	| M A X 
	| M I N 
	| S L O P E 
	| S T D E V 
	| S U M 
	| V A R
	;

// Opperators //
PLUS : '+';
MINUS : '-';
MUL : '*';
DIV : '/';
DIV2 : '/?';
MOD : '%';
POW : '^';
EQ : '==';
NEQ : '!=';
LT : '<';
LTE : '<=';
GT : '>';
GTE : '>=';
INC : '+=';
DEC : '-=';
AUTO_INC : '++';
AUTO_DEC : '--';
NULL_OP : '??';
IF_OP : '?';
ELSE_OP : ':';
LPAREN : '(';
RPAREN : ')';
LBRAC : '[';
RBRAC : ']';
LCURL : '{';
RCURL : '}';
COMMA : ',';
SEMI_COLON : ';';
CAST : '->';
LAMBDA : '=>';
DOT : '.';
ASSIGN : '=';
TILDA : '~';
OR : O R;
AND : A N D;
XOR : X O R;
NOT : N O T | '!';

// Keywods //
DEBUG_DUMP : '%' D U M P;
K_AGGREGATE : A G G R E G A T E;
K_ALL : A L L;
K_ANTI : A N T I;
K_APPEND : A P P E N D;
K_AS : A S;
K_ASC : A S C;
K_BY : B Y;
K_CASE : C A S E;
K_CONNECT : C O N N E C T;
K_CREATE : C R E A T E;
K_DECLARE : D E C L A R E;
K_DELETE : D E L E T E;
K_DESC : D E S C;
K_DISCONNECT : D I S C O N N E C T;
K_DO : D O;
K_DYNAMIC : D Y N A M I C;
K_ELSE : E L S E;
K_END : E N D;
K_ESCAPE : E S C A P E;
K_EXEC : E X E C;
K_FOR : F O R;
K_FROM : F R O M;
K_FULL : F U L L;
K_GRADIENT : G R A D I E N T;
K_HINT : H I N T;
K_IDENTITY : I D E N T I T Y | I D E N T;
K_IF : I F;
K_IN : I N;
K_INITIAL : I N I T | I N I T I A L;
K_INNER : I N N E R;
K_IS : I S;
K_JOIN : J O I N;
K_LAMBDA : L A M B D A;
K_LEFT : L E F T;
K_MAP : M A I N | M A P;
K_MERGE : M E R G E;
K_NEW : N E W;
K_NOT : N O T;
K_OF : O F;
K_ON : O N;
K_OVER : O V E R;
K_PARTITIONS : P A R T I T I O N S | T H R E A D S;
K_PRINT : P R I N T;
K_READ : R E A D | S E L E C T;
K_REDUCE : R E D U C E;
K_RETAIN : R E T A I N;
K_RIGHT : R I G H T;
K_SCRIPT : S C R I P T;
K_SET : S E T;
K_SIZE : S I Z E;
K_SORT : S O R T;
K_STATIC : S T A T I C;
K_TABLE : T A B L E;
K_THEN : T H E N;
K_TO : T O;
K_UPDATE : U P D A T E;
K_WHEN : W H E N;
K_WHERE : W H E R E;
K_WITH : W I T H;
K_WHILE : W H I L E;

// Core types //
T_BLOB : B L O B;
T_BOOL : B O O L;
T_DATE : D A T E;
T_DOUBLE : D O U B L E;
T_INT : I N T;
T_STRING : S T R I N G;

// Cell Literal Support //
LITERAL_NULL // NULL INT
	: N U L L
	; 
LITERAL_BOOL 
	: T R U E 
	| F A L S E
	;
LITERAL_BLOB 
	: '0' X (HEX HEX)*;
LITERAL_DATE 
	: '\'' DIGIT+ '-' DIGIT+ '-' DIGIT+ '\'' T 												// 'YYYY-MM-DD'T
	| '\'' DIGIT+ '-' DIGIT+ '-' DIGIT+ ':' DIGIT+ ':' DIGIT+ ':' DIGIT+ '\'' T				// 'YYYY-MM-DD:HH:MM:SS'T
	| '\'' DIGIT+ '-' DIGIT+ '-' DIGIT+ ':' DIGIT+ ':' DIGIT+ ':' DIGIT+ '.' DIGIT+ '\'' T	// 'YYYY-MM-DD:HH:MM:SS.LLLLLLLL'T
	;
LITERAL_DOUBLE 
	: DIGIT+ '.' DIGIT+ (D)?  // DOUBLE
	| (DIGIT+) D			// 'D' MEANS THIS HAS THE FORM OF AN INT, BUT WE WANT IT TO BE A DOUBLE; AVOIDS HAVING TO DO A CAST
	;
LITERAL_INT 
	: DIGIT+ // '~' INDICATES A NEGATIVE NUMBER
	;
LITERAL_STRING 
	: '\'' ( ~'\'' | '\'\'' )* '\'' // NORMAL STRING -> 'abcdef'
	| '"' ( ~'"' | '""')* '"'		// NORMAL STRING -> "ABCDEF"
	| '\'\''						// EMPTY STRING -> ''
	| SLIT .*? SLIT					// COMPLEX STRING LITERAL $$ ANYTHING $$
	| C R L F						// \n
	| T A B							// \t
	;

// Command Term //
CTERM : '%;' | G O ';';

// Identifier Parameter Text //
PARAMETER : '@' IDENTIFIER;
IDENTIFIER : [a-zA-Z_] [a-zA-Z_0-9]*;

// Comments and whitespace //
SINGLE_LINE_COMMENT : '//' ~[\r\n]* -> channel(HIDDEN);
MULTILINE_COMMENT : '/*' .*? ( '*/' | EOF ) -> channel(HIDDEN);
WS : ( ' ' | '\t' |'\r' | '\n' | '\r\n')* -> channel(HIDDEN);

fragment SLIT : '$$';
fragment DIGIT : [0-9];
fragment HEX : [aAbBcCdDeEfF0123456789];
fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];
