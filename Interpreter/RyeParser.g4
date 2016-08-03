parser grammar RyeParser;


options
{
	tokenVocab = RyeLexer;
}

compile_unit : command_set EOF;

// Commands //
command_set 
	: command (command)*
	;

command
	: command_method
	| command_declare
	| command_create
	| command_read
	| command_aggregate
	| command_update
	| command_delete
	| command_join
	| command_connect
	| command_disconnect
	| command_sort
	| command_debug
	;

// Debug //
command_debug
	: DEBUG_DUMP table_name COMMA expression SEMI_COLON;

// ------------------------------------------ Connect / Disconnect ------------------------------------------ //
command_connect
	: K_CONNECT LCURL (connect_unit SEMI_COLON)+ RCURL SEMI_COLON
	;
command_disconnect
	: K_DISCONNECT LCURL (IDENTIFIER SEMI_COLON)+ RCURL SEMI_COLON
	; 
connect_unit
	: IDENTIFIER (COMMA | K_TO)? expression
	;

// ------------------------------------------ Create Table ------------------------------------------ //
command_create
	: K_CREATE (K_TABLE)? table_name (K_SIZE LITERAL_INT)? 
	LCURL 
		create_unit (COMMA create_unit)* 
	RCURL SEMI_COLON
	;
create_unit
	: IDENTIFIER K_AS type
	;

// ------------------------------------------ Declare ------------------------------------------ //
command_declare
	: K_DECLARE LCURL ((unit_declare_scalar | unit_declare_matrix | unit_declare_lambda) SEMI_COLON)+ RCURL SEMI_COLON
	;
unit_declare_scalar 
	: IDENTIFIER DOT IDENTIFIER K_AS type (ASSIGN expression)
	;
unit_declare_matrix 
	: IDENTIFIER DOT IDENTIFIER LBRAC (expression (COMMA expression)?)? RBRAC K_AS type (ASSIGN matrix_expression)?
	;
unit_declare_lambda
	: lambda_name LPAREN (IDENTIFIER (COMMA IDENTIFIER)*)? RPAREN K_AS K_LAMBDA ASSIGN expression			// GLOBAL.SQUARE(X,Y) AS LAMBDA = X^2 + Y^2;
	| lambda_name K_AS K_LAMBDA ASSIGN K_GRADIENT K_OF lambda_name K_OVER IDENTIFIER						// LAMBDA GLOBAL.RATE AS GRADIENT GLOBAL.SQUARE OVER X; WHICH WOULD YEILD 2X
	;
lambda_name
	: IDENTIFIER DOT IDENTIFIER
	;
// ------------------------------------------ Sort ------------------------------------------ //
command_sort
	: K_SORT LCURL 
		K_FROM table_name (K_AS IDENTIFIER)? SEMI_COLON
	RCURL SEMI_COLON
	K_BY LCURL
		sort_unit (COMMA sort_unit)* SEMI_COLON
	RCURL SEMI_COLON
	;
sort_unit : expression (K_ASC | K_DESC)?;

// ------------------------------------------ Read ------------------------------------------ //
command_read
	: K_READ 
	base_clause
	(by_clause)?
	(command_declare)?
	map_clause
	(reduce_clause)?
	;
map_clause 
	: K_MAP LCURL method* RCURL SEMI_COLON
	;
reduce_clause
	: K_REDUCE LCURL method* RCURL SEMI_COLON
	;

// ------------------------------------------ Aggregate ------------------------------------------ //
command_aggregate
	: K_AGGREGATE
	base_clause
	(by_clause)?
	(over_clause)?
	append_method
	;
over_clause
	: (K_OVER LCURL beta_reduction_list SEMI_COLON RCURL SEMI_COLON)
	;

// ------------------------------------------ Update ------------------------------------------ //
command_update
	: K_UPDATE
	base_clause
	K_SET LCURL (IDENTIFIER ASSIGN expression SEMI_COLON)+ RCURL SEMI_COLON
	;

// ------------------------------------------ Delete ------------------------------------------ //
command_delete
	: K_DELETE
	base_clause
	;

// Union //

// ------------------------------------------ Join ------------------------------------------ //
command_join
	: K_JOIN
	LCURL 
		K_FROM table_name (K_AS IDENTIFIER)? SEMI_COLON
		K_FROM table_name (K_AS IDENTIFIER)? SEMI_COLON
		(where_clause SEMI_COLON)? 
		(thread_clause SEMI_COLON)? 
		(join_type SEMI_COLON)?
	RCURL SEMI_COLON
	(K_ON LCURL (join_on_unit SEMI_COLON)+ RCURL SEMI_COLON)?
	append_method
	;

join_type
	: K_INNER | K_LEFT | K_RIGHT | K_ANTI K_INNER | K_ANTI K_LEFT | K_ANTI K_RIGHT | K_FULL | K_CROSS
	;
join_on_unit
	: IDENTIFIER DOT IDENTIFIER (EQ | ASSIGN | K_TO) IDENTIFIER DOT IDENTIFIER
	;

// ------------------------------------------ Query support ------------------------------------------ //
by_clause
	: K_BY LCURL expression_or_wildcard_set SEMI_COLON RCURL SEMI_COLON
	;
base_clause
	: LCURL 
		K_FROM table_name (K_AS IDENTIFIER)? SEMI_COLON 
		(where_clause SEMI_COLON)? 
		(thread_clause SEMI_COLON)? 
		RCURL SEMI_COLON
	;

// ----------------------------------------------------------------------------------------------------- //
// ---------------------------------------------- ACTIONS ---------------------------------------------- //
// ----------------------------------------------------------------------------------------------------- //

// Actions //
command_method : method;
method
	: variable_assign																									# ActScalarAssign // x = y
	| K_PRINT expression_or_wildcard_set SEMI_COLON																		# ActPrint // print 'hello world!!!';
	| K_PRINT matrix_expression SEMI_COLON																				# ActPrintMat // Print matrix[]
	| K_PRINT K_LAMBDA IDENTIFIER DOT IDENTIFIER SEMI_COLON																# ActPrintLambda
	| append_method																										# ActAppend // Return A, B AS C, D * E / F AS G
	| K_ESCAPE K_FOR SEMI_COLON																							# ActEscapeFor
	| K_ESCAPE K_READ SEMI_COLON																						# ActEscapeRead
	| matrix_name ASSIGN matrix_expression SEMI_COLON																	# ActMatAssign
	| matrix_unit_assign																								# ActMatUnitAssign
	| structure_method_weak																								# ActSMWeak
	| structure_method_strict																							# ActSMStrict
	
	| K_DO LCURL (method)+ RCURL SEMI_COLON																				# ActBeginEnd // Begin <...> End
	| K_IF expression K_THEN method (K_ELSE method)?																	# ActIf // IF t == v THEN (x++) ELSE (x--)
	| K_FOR IDENTIFIER DOT IDENTIFIER ASSIGN expression K_TO expression method											# ActFor // For T = 0 to 10 (I++,I--)
	| K_WHILE expression method																							# ActWhile
	;

// Append table method //
append_method
	: K_APPEND LCURL (K_NEW)? table_name SEMI_COLON (K_RETAIN expression_or_wildcard_set SEMI_COLON) RCURL SEMI_COLON
	;

// Structure Methods //
structure_method_weak
	: IDENTIFIER DOT IDENTIFIER (method_param (COMMA method_param)*)? SEMI_COLON
	;
structure_method_strict
	: IDENTIFIER DOT IDENTIFIER LCURL method_param_named+ RCURL SEMI_COLON
	;
method_param_named
	: IDENTIFIER ASSIGN method_param SEMI_COLON
	;
method_param
	: expression									// E
	| LCURL expression_or_wildcard_set RCURL		// V
	| K_TABLE table_name (K_AS IDENTIFIER)?			// T
	| matrix_expression								// M
	| DOT											// NIL
	;

// Matrix //
matrix_unit_assign
	: IDENTIFIER DOT IDENTIFIER LBRAC expression COMMA expression RBRAC ASSIGN expression SEMI_COLON		# MUnit2DAssign
	| IDENTIFIER DOT IDENTIFIER LBRAC expression COMMA expression RBRAC INC expression SEMI_COLON			# MUnit2DInc
	| IDENTIFIER DOT IDENTIFIER LBRAC expression COMMA expression RBRAC AUTO_INC SEMI_COLON					# MUnit2DAutoInc
	| IDENTIFIER DOT IDENTIFIER LBRAC expression COMMA expression RBRAC DEC expression SEMI_COLON			# MUnit2DDec
	| IDENTIFIER DOT IDENTIFIER LBRAC expression COMMA expression RBRAC AUTO_DEC SEMI_COLON					# MUnit2DAutoDec

	| IDENTIFIER DOT IDENTIFIER LBRAC expression RBRAC ASSIGN expression SEMI_COLON							# MUnit1DAssign
	| IDENTIFIER DOT IDENTIFIER LBRAC expression RBRAC INC expression SEMI_COLON							# MUnit1DInc
	| IDENTIFIER DOT IDENTIFIER LBRAC expression RBRAC AUTO_INC SEMI_COLON									# MUnit1DAutoInc
	| IDENTIFIER DOT IDENTIFIER LBRAC expression RBRAC DEC expression SEMI_COLON							# MUnit1DDec
	| IDENTIFIER DOT IDENTIFIER LBRAC expression RBRAC AUTO_DEC SEMI_COLON									# MUnit1DAutoDec

	| IDENTIFIER DOT IDENTIFIER LBRAC RBRAC ASSIGN expression SEMI_COLON									# MAllAssign
	| IDENTIFIER DOT IDENTIFIER LBRAC RBRAC INC expression SEMI_COLON										# MAllInc
	| IDENTIFIER DOT IDENTIFIER LBRAC RBRAC AUTO_INC SEMI_COLON												# MAllAutoInc
	| IDENTIFIER DOT IDENTIFIER LBRAC RBRAC DEC expression SEMI_COLON										# MAllDec
	| IDENTIFIER DOT IDENTIFIER LBRAC RBRAC AUTO_DEC SEMI_COLON												# MAllAutoDec

	;

// Assign //
variable_assign
	: IDENTIFIER DOT IDENTIFIER ASSIGN expression SEMI_COLON		# ActAssign
	| IDENTIFIER DOT IDENTIFIER INC expression SEMI_COLON			# ActInc
	| IDENTIFIER DOT IDENTIFIER AUTO_INC SEMI_COLON					# ActAutoInc
	| IDENTIFIER DOT IDENTIFIER DEC expression SEMI_COLON			# ActDec
	| IDENTIFIER DOT IDENTIFIER AUTO_DEC SEMI_COLON					# ActAutoDec
	;

// ----------------------------------------------------------------------------------------------------- //
// ----------------------------------------------- MATRIX ---------------------------------------------- //
// ----------------------------------------------------------------------------------------------------- //

 matrix_expression
	: MINUS matrix_expression															# MatrixMinus
	| NOT matrix_expression																# MatrixInvert
	| TILDA matrix_expression															# MatrixTranspose
	| matrix_expression MUL MUL matrix_expression										# MatrixTrueMul

	| matrix_expression op=(MUL | DIV | DIV2) matrix_expression							# MatrixMulDiv
	| matrix_expression op=(MUL | DIV | DIV2) expression								# MatrixMulDivLeft
	| expression op=(MUL | DIV | DIV2) matrix_expression								# MatrixMulDivRight

	| matrix_expression op=(PLUS | MINUS) matrix_expression								# MatrixAddSub
	| expression op=(PLUS | MINUS) matrix_expression									# MatrixAddSubLeft
	| matrix_expression op=(PLUS | MINUS) expression									# MatrixAddSubRight

	| matrix_name																		# MatrixLookup
	| matrix_literal																	# MatrixLiteral
	| K_IDENTITY LPAREN type COMMA expression RPAREN									# MatrixIdent

	| LPAREN matrix_expression RPAREN													# MatrixParen
	;

matrix_name 
	: IDENTIFIER DOT IDENTIFIER LBRAC RBRAC
	;
matrix_literal 
	: vector_literal (COMMA vector_literal)*
	;
vector_literal 
	: LCURL expression (COMMA expression)* RCURL
	;

// ----------------------------------------------------------------------------------------------------- //
// --------------------------------------------- AGGREGATES -------------------------------------------- //
// ----------------------------------------------------------------------------------------------------- //

 beta_reduction_list 
	: beta_reduction (COMMA beta_reduction)*
	;
 beta_reduction
	: SET_REDUCTIONS LPAREN (expression_alias_list)? RPAREN (where_clause)? (K_AS IDENTIFIER)?
	;

// ----------------------------------------------------------------------------------------------------- //
// --------------------------------------------- EXPRESSIONS ------------------------------------------- //
// ----------------------------------------------------------------------------------------------------- //

 // Return Expression // 
expression_or_wildcard_set 
	: expression_or_wildcard (COMMA expression_or_wildcard)*
	;
expression_or_wildcard
	: expression_alias
	| IDENTIFIER DOT MUL
	; 

// Where Clause //
where_clause 
	: K_WHERE expression
	;

// Threading //
thread_clause
	: K_PARTITIONS ASSIGN (K_MAX | LITERAL_INT)
	;

// Expression Lists //
expression_alias_list 
	: expression_alias (COMMA expression_alias)*
	;
expression_list 
	: expression (COMMA expression)*
	;

// Expressions //
expression_alias 
	: expression (K_AS IDENTIFIER)?
	;
expression
	: IDENTIFIER DOT type																				# Pointer // X.STRING.5
	| op=(NOT | PLUS | MINUS) expression																# Uniary
	| expression POW expression																			# Power
	| expression op=(MUL | DIV | MOD | DIV2) expression													# MultDivMod
	| expression op=(PLUS | MINUS) expression															# AddSub
	| expression op=(GT | GTE | LT | LTE) expression													# GreaterLesser
	| expression op=(EQ | NEQ) expression																# Equality
	| expression K_IS LITERAL_NULL																		# IsNull
	| expression AND expression																			# LogicalAnd
	| expression op=(OR | XOR) expression																# LogicalOr
	| expression CAST type  																			# Cast
	| variable																							# ExpressionVariable
	| cell																								# Static
	| expression NULL_OP expression																		# IfNullOp
	| expression IF_OP expression (ELSE_OP expression)?													# IfOp
	| K_CASE (K_WHEN expression K_THEN expression)+ (K_ELSE expression)? K_END							# CaseOp
	| system_function LPAREN ( expression ( COMMA expression )* )? RPAREN								# SystemFunction
	| structure_function LPAREN ( expression ( COMMA expression )* )? RPAREN							# StructureFunction
	| IDENTIFIER DOT IDENTIFIER LBRAC expression COMMA expression RBRAC									# Matrix2D
	| IDENTIFIER DOT IDENTIFIER LBRAC expression RBRAC													# Matrix1D
	| IDENTIFIER LBRAC expression COMMA expression RBRAC												# Matrix2DNaked
	| IDENTIFIER LBRAC expression RBRAC																	# Matrix1DNaked
	| LPAREN expression RPAREN																			# Parens
	;

// ----------------------------------------------------------------------------------------------------- //
// --------------------------------------------- BASE SUPPORT ------------------------------------------ //
// ----------------------------------------------------------------------------------------------------- //

/*
	Variables:
	FieldName -> look only at the table
	TableName.FieldName -> look only at table
	Global.FieldName -> look at global heap
	Local.FieldName -> look at local heap
*/
variable
	: IDENTIFIER					# VariableNaked
	| IDENTIFIER DOT IDENTIFIER		# SpecificVariable
	;

// Cell Logic //
cell
	: LITERAL_BOOL			# CellLiteralBool
	| LITERAL_INT			# CellLiteralInt
	| LITERAL_DOUBLE		# CellLiteralDouble
	| LITERAL_DATE			# CellLiteralDate
	| LITERAL_STRING		# CellLiteralString
	| LITERAL_BLOB			# CellLiteralBLOB
	| LITERAL_NULL			# CellNull
	;

// Functions //
system_function
	: IDENTIFIER
	;
structure_function
	: IDENTIFIER DOT IDENTIFIER
	;

// Table logic //  
table_name 
	: IDENTIFIER DOT IDENTIFIER
	;

// Types //
type : (T_BLOB | T_BOOL | T_DATE | T_DOUBLE | T_INT | T_STRING) (DOT LITERAL_INT)?;
