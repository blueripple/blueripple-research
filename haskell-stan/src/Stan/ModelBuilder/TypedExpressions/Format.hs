{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# LANGUAGE TypeApplications #-}

module Stan.ModelBuilder.TypedExpressions.Format
  (
    module Stan.ModelBuilder.TypedExpressions.Format
  )
  where

import Stan.ModelBuilder.TypedExpressions.Recursion
import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.TypedList
import Stan.ModelBuilder.TypedExpressions.Indexing
import Stan.ModelBuilder.TypedExpressions.Operations
import Stan.ModelBuilder.TypedExpressions.Functions
import Stan.ModelBuilder.TypedExpressions.Expressions
import Stan.ModelBuilder.TypedExpressions.Statements

import qualified Data.Functor.Foldable as RS
import qualified Data.Foldable as Foldable

import qualified Data.IntMap.Strict as IM
import qualified Data.List as List
import qualified Data.List.NonEmpty as NE
import Data.List ((!!))
import qualified Data.Vec.Lazy as DT
import qualified Data.Type.Nat as DT
import qualified Data.Fin as DT
import qualified Data.Text as T

import qualified Stan.ModelBuilder.Expressions as SME
import Prelude hiding (Nat)
import Relude.Extra
import qualified Data.Map.Strict as Map

import qualified Prettyprinter as PP
import Prettyprinter ((<+>))
import qualified Data.GADT.Compare as DT
import qualified Prettyprinter.Render.Text as PP
--import qualified Data.List.NonEmpty.Extra as List


type CodePP = PP.Doc ()

-- we replace LExprs within statements with prettyprinted code
-- then fold up the statements to produce code
stmtToCodeE :: LStmt -> Either Text CodePP
stmtToCodeE = RS.hylo stmtToCodeAlg (hfmap exprToCode . RS.project)

stmtToCodeAlg :: StmtF (K CodePP) (Either Text CodePP) -> Either Text CodePP
stmtToCodeAlg = \case
  SDeclareF txt st divf vms -> Right $ stanDeclHead st (unK <$> DT.toList (unDeclIndexVecF divf)) vms <+> PP.pretty txt <> PP.semi
  SDeclAssignF txt st divf vms rhs -> Right $ stanDeclHead st (unK <$> DT.toList (unDeclIndexVecF divf)) vms <+> PP.pretty txt <+> PP.equals <+> unK rhs <> PP.semi
  SAssignF lhs rhs -> Right $ unK lhs <+> PP.equals <+> unK rhs <> PP.semi
  STargetF rhs -> Right $ "target +=" <+> unK rhs <> PP.semi
  SSampleF lhs (Density dn _ _ rF) al -> Right $ unK lhs <+> "~" <+> PP.pretty dn <> PP.parens (csArgList $ rF al) <> PP.semi
  SForF txt fe te body -> (\b -> "for" <+> PP.parens (PP.pretty txt <+> "in" <+> unK fe <> PP.colon <> unK te) <+> bracketBlock b) <$> sequenceA body
  SForEachF txt e body -> (\b -> "foreach" <+> PP.parens (PP.pretty txt <+> "in" <+> unK e) <+> bracketBlock b) <$> sequence body
  SIfElseF condAndIfTrueL allFalse -> ifElseCode condAndIfTrueL allFalse
  SWhileF if' body -> (\b -> "while" <+> PP.parens (unK if') <+> bracketBlock b) <$> sequence body
  SBreakF -> Right $ "break"
  SContinueF -> Right $ "continue"
  SFunctionF (Function fname rt ats rF) al body re ->
    (\b -> PP.pretty (sTypeName rt) <+> PP.pretty fname <> functionArgs (applyTypedListFunctionToTypeList rF ats) (rF al)
           <+> bracketBlock (b `appendAsList` ["return" <+> unK re <> PP.semi])) <$> sequence body
  SFunctionF (IdentityFunction _) _ _ _ -> Left "Attempt to *declare* Identity function!"
  SCommentF cs -> case toList cs of
    [] -> Right mempty
    [c] -> Right $ "//" <+> PP.pretty c
    cs -> Right $ "{*" <> PP.line <> PP.indent 2 (PP.vsep $ PP.pretty <$> cs) <> PP.line <> "*}"
  SProfileF t body -> (\b -> "profile" <> PP.parens (PP.dquotes $ PP.pretty t) <+> bracketBlock b) <$> sequenceA body
  SPrintF al -> Right $ "print" <+> PP.parens (csArgList al) <> PP.semi
  SRejectF al -> Right $ "reject" <+> PP.parens (csArgList al) <> PP.semi
  SScopedF body -> bracketBlock <$> sequence body
  SBlockF bl body -> maybe (Right mempty) (fmap (\x -> PP.pretty (stmtBlockHeader bl) <> bracketBlock x) . sequenceA) $ nonEmpty $ toList body
  SContextF mf body -> case mf of
    Just _ -> Left "stmtToCodeAlg: Impossible! SContext with a change function remaining!"
    Nothing -> blockCode <$> sequence body

indexCodeL :: [CodePP] -> CodePP
indexCodeL [] = ""
indexCodeL x = PP.brackets $ PP.hsep $ PP.punctuate "," x

stanDeclHead :: StanType t -> [CodePP] -> [VarModifier (K CodePP) (ScalarType t)] -> CodePP
stanDeclHead st il vms = case st of
  StanArray sn st -> let (adl, sdl) = List.splitAt (fromIntegral $ DT.snatToNatural sn) il
                     in "array" <> indexCodeL adl <+> stanDeclHead st sdl vms
  _ -> PP.pretty (stanTypeName st)  <> indexCodeL il <> varModifiersToCode vms
  where
    vmToCode = \case
      VarLower x -> "lower" <> PP.equals <> unK x
      VarUpper x -> "upper" <> PP.equals <> unK x
      VarOffset x -> "offset" <> PP.equals <> unK x
      VarMultiplier x -> "multiplier" <> PP.equals <> unK x
    varModifiersToCode vms =
      if null vms
      then mempty
      else PP.langle <> (PP.hsep $ PP.punctuate ", " $ fmap vmToCode vms) <> PP.rangle

-- add brackets and indent the lines of code
bracketBlock :: Traversable f => f CodePP -> CodePP
bracketBlock = bracketCode . blockCode

-- surround with brackets and indent
bracketCode :: CodePP -> CodePP
bracketCode c = "{" <> PP.line <> PP.indent 2 c <> PP.line <> "} "

addSemi :: CodePP -> CodePP
addSemi c = c <> PP.semi

-- put each item of code on a separate line
blockCode :: Traversable f => f CodePP -> CodePP
blockCode ne = PP.vsep $ toList ne

-- put each line *after the first* on a new line
blockCode' :: Traversable f => f CodePP -> CodePP
blockCode' cs = case toList cs of
  [] -> mempty
  (c : cs) -> c <> PP.vsep cs

appendAsList :: Traversable f => f a -> [a] -> [a]
appendAsList fa as = toList fa ++ as

functionArgs:: TypeList args -> TypedList (FuncArg Text) args -> CodePP
functionArgs argTypes argNames = PP.parens $ mconcat $ PP.punctuate (PP.comma <> PP.space) argCodeList
  where
    handleFA :: CodePP -> FuncArg Text t -> CodePP
    handleFA c = \case
      Arg a -> c <+> PP.pretty a
      DataArg a -> "data" <+> c <+> PP.pretty a

    arrayIndices :: SNat n -> CodePP
    arrayIndices sn = if n == 0 then mempty else PP.brackets (mconcat $ List.replicate (n-1) PP.comma)
      where n = fromIntegral $ DT.snatToNatural sn

    handleType :: SType t -> CodePP
    handleType st = case st of
      SArray sn st -> "array" <> arrayIndices sn <+> handleType st
      _ -> PP.pretty $ sTypeName st

    f :: SType t -> FuncArg Text t -> K CodePP t
    f st fa = K $ handleFA (handleType st) fa

    argCodeList = typedKToList $ zipTypedListsWith f (typeListToSTypeList argTypes) argNames

-- This might be wrong after switch from NE to
ifElseCode :: NonEmpty (K CodePP EBool, Either Text CodePP) -> Either Text CodePP -> Either Text CodePP
ifElseCode ecNE c = do
  let appendNEList :: NonEmpty a -> [a] -> NonEmpty a
      appendNEList (a :| as) as' = a :| as ++ as'
      (conds, ifTrueCodeEs) = NE.unzip ecNE
      condCode (e :| es) = "if" <+> PP.parens (unK e) <> PP.space :| fmap (\le -> "else if" <+> PP.parens (unK le) <> PP.space) es
      condCodeNE = condCode conds `appendNEList` ["else"]
  ifTrueCodes <- sequenceA (ifTrueCodeEs `appendNEList` [c])
  let ecNE = NE.zipWith (<+>) condCodeNE (fmap (bracketBlock . pure @[]) ifTrueCodes)
  return $ blockCode' ecNE

data  IExprCode :: Type where
  Bare :: CodePP -> IExprCode
  Oped :: CodePP -> IExprCode -- has a binary operator in it so might need parentheses
  Indexed :: CodePP -> [Int] -> IM.IntMap IExprCode -> IExprCode -- needs indexing. Carries *already sliced* indices and index expressions

data IExprCodeF :: Type -> Type where
  BareF :: CodePP -> IExprCodeF a
  OpedF :: CodePP -> IExprCodeF a
  IndexedF :: CodePP -> [Int] -> IM.IntMap a -> IExprCodeF a

instance Functor IExprCodeF where
  fmap f = \case
    BareF doc -> BareF doc
    OpedF c -> OpedF c
    IndexedF c si im -> IndexedF c si (f <$> im)

instance Foldable IExprCodeF where
  foldMap f = \case
    BareF doc -> mempty
    OpedF _ -> mempty
    IndexedF _ _ im -> foldMap f im

instance Traversable IExprCodeF where
  traverse g = \case
    BareF doc -> pure $ BareF doc
    OpedF c -> pure $ OpedF c
    IndexedF c si im -> IndexedF c si <$> traverse g im

type instance RS.Base IExprCode = IExprCodeF

instance RS.Recursive IExprCode where
  project = \case
    Bare doc -> BareF doc
    Oped c -> OpedF c
    Indexed iec si im -> IndexedF iec si im

instance RS.Corecursive IExprCode where
  embed = \case
    BareF doc -> Bare doc
    OpedF c -> Oped c
    IndexedF doc si im -> Indexed doc si im

iExprToDocAlg :: IExprCodeF CodePP -> CodePP
iExprToDocAlg = \case
  BareF doc -> doc
  OpedF c -> c
  IndexedF doc _ im -> doc <> PP.brackets (mconcat $ PP.punctuate ", " $ withLeadingEmpty im)

withLeadingEmpty :: IntMap CodePP -> [CodePP]
withLeadingEmpty im = imWLE where
  minIndex = Foldable.minimum $ IM.keys im
  imWLE = List.replicate minIndex mempty ++ IM.elems im

iExprToCode :: IExprCode -> CodePP
iExprToCode = RS.cata iExprToDocAlg

csArgList :: TypedList (K CodePP) args -> CodePP
csArgList = mconcat . PP.punctuate (PP.comma <> PP.space) . typedKToList

-- I am not sure about/do not understand the quantified constraint here.
exprToDocAlg :: IAlg LExprF (K IExprCode) -- LExprF ~> K IExprCode
exprToDocAlg = K . \case
  LNamed txt st -> Bare $ PP.pretty txt
  LInt n -> Bare $ PP.pretty n
  LReal x -> Bare $ PP.pretty x
  LComplex x y -> Bare $ PP.parens $ PP.pretty x <+> "+" <+> "i" <> PP.pretty y -- (x + iy))
  LString t -> Bare $ PP.dquotes $ PP.pretty t
  LVector xs -> Bare $ PP.brackets $ PP.pretty $ T.intercalate ", " (show <$> xs)
  LMatrix ms -> Bare $ unNestedToCode PP.brackets [length ms] $ PP.pretty <$> concatMap DT.toList ms--PP.brackets $ PP.pretty $ T.intercalate "," $ fmap (T.intercalate "," . fmap show . DT.toList) ms
  LArray nv -> Bare $ nestedVecToCode nv
  LIntRange leM ueM -> Oped $ maybe mempty (unK . f) leM <> PP.colon <> maybe mempty (unK . f) ueM
  LFunction (Function fn _ _ rF) al -> Bare $ PP.pretty fn <> PP.parens (csArgList $ hfmap f $ rF al)
  LFunction (IdentityFunction _) (arg :> TNil) -> Bare $ unK $ f arg
  LDensity (Density dn _ _ rF) k al -> Bare $ PP.pretty dn <> PP.parens (unK (f k) <> PP.pipe <+> csArgList (hfmap f $ rF al))
  LBinaryOp sbo le re -> Oped $ unK (f $ parenthesizeOped le) <+> opDoc sbo <+> unK (f $ parenthesizeOped re)
  LUnaryOp op e -> Bare $ unaryOpDoc (unK (f $ parenthesizeOped e)) op
  LCond ce te fe -> Bare $ unK (f ce) <+> "?" <+> unK (f te) <+> PP.colon <+> unK (f fe)
  LSlice sn ie e -> sliced sn ie e
  LIndex sn ie e -> indexed sn ie e
  where
    f :: K IExprCode ~> K CodePP
    f = K . iExprToCode . unK
    parenthesizeOped :: K IExprCode ~> K IExprCode
    parenthesizeOped x = case unK x of
       Oped doc -> K $ Oped $ PP.parens doc
       x -> K x
    addSlice :: SNat n -> K IExprCode EInt -> K IExprCode d -> [Int] -> IM.IntMap IExprCode -> ([Int], IM.IntMap IExprCode)
    addSlice sn kei ke si im = (si', im')
      where
        newIndex :: Int = fromIntegral $ DT.snatToNatural sn
        origIndex = let f n = if n `elem` si then f (n + 1) else n in f newIndex -- find the correct index in the original
        si' = origIndex : si
        im' = IM.alter (Just . maybe (unK kei) (sliced s0 kei . K)) origIndex im
    sliced :: SNat n -> K IExprCode EInt -> K IExprCode t -> IExprCode
    sliced sn kei ke = case unK ke of
      Bare c -> let (si, im) = addSlice sn kei ke [] IM.empty in Indexed c si im
      Oped c -> let (si, im) = addSlice sn kei ke [] IM.empty in Indexed (PP.parens c) si im
      Indexed c si im -> let (si', im') = addSlice sn kei ke si im in Indexed c si' im'
    addIndex :: SNat n -> K IExprCode (EArray (S Z) EInt) -> K IExprCode d -> [Int] -> IM.IntMap IExprCode -> IM.IntMap IExprCode
    addIndex sn kre ke si im = im'
      where
        newIndex :: Int = fromIntegral $ DT.snatToNatural sn
        origIndex = let f n = if n `elem` si then f (n + 1) else n in f newIndex
        im' = IM.alter (Just . maybe (unK kre) (indexed s0 kre . K)) origIndex im
    indexed :: SNat n -> K IExprCode (EArray (S Z) EInt) -> K IExprCode t -> IExprCode
    indexed sn kei ke = case unK ke of
      Bare c -> Indexed c [] $ addIndex sn kei ke [] IM.empty
      Oped c -> Indexed (PP.parens c) [] $ addIndex sn kei ke [] IM.empty
      Indexed c si im -> Indexed c si $ addIndex sn kei ke si im

exprToIExprCode :: LExpr ~> K IExprCode
exprToIExprCode = iCata exprToDocAlg

exprToCode :: LExpr ~> K CodePP
exprToCode = K . iExprToCode . unK . exprToIExprCode

unaryOpDoc :: CodePP -> SUnaryOp op -> CodePP
unaryOpDoc ed = \case
  SNegate -> "-" <> ed
  STranspose -> ed <> "'"

boolOpDoc :: SBoolOp op -> CodePP
boolOpDoc = \case
  SEq -> PP.equals <> PP.equals
  SNEq -> "!="
  SLT -> PP.langle
  SLEq -> "<="
  SGT -> PP.rangle
  SGEq -> ">="
  SAnd -> "&&"
  SOr -> "||"

opDoc :: SBinaryOp op -> CodePP
opDoc = \case
  SAdd ->  "+"
  SSubtract -> "-"
  SMultiply -> "*"
  SDivide -> PP.slash
  SPow -> "^"
  SModulo -> "%"
  SElementWise sbo -> PP.dot <> opDoc sbo
--  SAndEqual sbo -> opDoc sbo <> PP.equals
  SBoolean bop -> boolOpDoc bop

nestedVecToCode :: NestedVec n (K IExprCode t) -> CodePP
nestedVecToCode nv = unNestedToCode PP.braces (drop 1 $ reverse dims) itemsCode
  where
    f = iExprToCode . unK
    (dims, items) = unNest nv
    itemsCode = f <$> items

-- given a way to surround a group,
-- a set of dimensions to group in order of grouping (so reverse order of left to right indexes)
-- items of code in one long list
-- produce a code item with them grouped
-- e.g., renderAsText $ unNestedCode PP.parens [2, 3] ["a","b","c","d","e","f"] = "((a, b), (c, d), (e, f))"
unNestedToCode :: (CodePP -> CodePP) -> [Int] -> [CodePP] -> CodePP
unNestedToCode surroundF dims items = surround $ go dims items
  where
    rdims = reverse dims
    surround = surroundF . PP.hsep . PP.punctuate ", "
    group :: Int -> [CodePP] -> [CodePP] -> [CodePP]
    group _ [] bs = bs
    group n as bs = let (g , as') = List.splitAt n as in group n as' (bs ++ [surround g]) -- this is quadratic. Fix.
    go :: [Int] -> [CodePP] -> [CodePP]
    go [] as = as
    go (x : xs) as = go xs (group x as [])

stmtBlockHeader :: StmtBlock -> Text
stmtBlockHeader = \case
  FunctionsStmts -> "functions"
  DataStmts -> "data"
  TDataStmts -> "transformed data"
  ParametersStmts -> "parameters"
  TParametersStmts -> "transformed parameters"
  ModelStmts -> "model"
  GeneratedQuantitiesStmts -> "generated quantities"

exprToText :: LExpr t -> Text
exprToText = PP.renderStrict . PP.layoutSmart PP.defaultLayoutOptions . unK . exprToCode

printLookupCtxt :: IndexLookupCtxt -> Text
printLookupCtxt (IndexLookupCtxt s i) = "sizes: " <> T.intercalate ", " (printF <$> Map.toList s)
                                        <> "indexes: " <> T.intercalate ", " (printF <$> Map.toList i)
  where
    printF :: forall t.(Text, LExpr t) -> Text
    printF (ik, le) = "("<> ik <> ", " <> exprToText le  <> ")"
