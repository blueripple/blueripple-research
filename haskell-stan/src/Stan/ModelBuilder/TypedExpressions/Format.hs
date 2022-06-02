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
{-# LANGUAGE TypeSynonymInstances #-}
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

import qualified Stan.ModelBuilder.Expressions as SME
import Prelude hiding (Nat)
import Relude.Extra
import qualified Data.Map.Strict as Map

import qualified Prettyprinter as PP
import Prettyprinter ((<+>))
--import qualified Data.List.NonEmpty.Extra as List


type CodePP = PP.Doc ()

stmtToCodeE :: LStmt -> Either Text CodePP
stmtToCodeE = RS.cata stmtToCodeAlg

stmtToCodeAlg :: StmtF LExpr (Either Text CodePP) -> Either Text CodePP
stmtToCodeAlg = \case
  SDeclareF txt st divf -> Right $ stanDeclHead st (DT.toList $ unDeclIndexVecF divf) <+> PP.pretty txt <> PP.semi
  SDeclAssignF txt st divf rhs -> Right $ stanDeclHead st (DT.toList $ unDeclIndexVecF divf) <+> PP.pretty txt <+> PP.equals <+> f rhs <> PP.semi
  SAssignF lhs rhs -> Right $ f lhs <+> PP.equals <+> f rhs <> PP.semi
  STargetF rhs -> Right $ "target +=" <+> f rhs
  SSampleF lhs (Distribution dn _ _) al -> Right $ f lhs <+> "~" <+> PP.pretty dn <> PP.parens (csArgList (hfmap exprToCode al))
  SForF txt fe te body -> (\b -> "for" <+> PP.parens (PP.pretty txt <+> "in" <+> f fe <> PP.colon <> f te) <+> bracketBlock b) <$> sequenceA body
  SForEachF txt e body -> (\b -> "foreach" <+> PP.parens (PP.pretty txt <+> "in" <+> f e) <+> bracketBlock b) <$> sequence body
  SIfElseF condAndIfTrueL allFalse -> ifElseCode condAndIfTrueL allFalse
  SWhileF if' body -> (\b -> "while" <+> PP.parens (f if') <+> bracketBlock b) <$> sequence body
  SBreakF -> Right $ "break" <> PP.semi
  SContinueF -> Right $ "continue" <> PP.semi
  SFunctionF (Function fname rt ats) al body re ->
    (\b -> PP.pretty (sTypeName rt) <+> "function" <+> PP.pretty fname <> functionArgs ats al <+> bracketBlock (b `appendList` ["return" <+> f re <> PP.semi])) <$> sequence body
  SPrintF al -> Right $ "print" <+> PP.parens (csArgList $ hfmap exprToCode al) <> PP.semi
  SRejectF al -> Right $ "reject" <+> PP.parens (csArgList $ hfmap exprToCode al) <> PP.semi
  SScopedF body -> bracketBlock <$> sequence body
  SContextF mf body -> case mf of
    Just _ -> Left $ "stmtToCodeAlg: Impossible! SContext with a change function remaining!"
    Nothing -> blockCode <$> sequence body
  where
    f = unK . exprToCode

indexCodeL :: [LExpr EInt] -> CodePP
indexCodeL [] = ""
indexCodeL (ie : ies) = PP.brackets $ mconcat $ PP.punctuate ", " $ go ie ies
  where
    go ie [] = [unK $ exprToCode ie]
    go ie (ie' : ies') = (unK $ exprToCode ie) : go ie' ies'

stanDeclHead :: StanType t -> [LExpr EInt] -> CodePP
stanDeclHead st il = case st of
  StanArray sn st -> let (adl, sdl) = List.splitAt (fromIntegral $ DT.snatToNatural sn) il
                     in "array" <> indexCodeL adl <+> stanDeclHead st sdl
  _ -> PP.pretty (stanTypeName st) <> indexCodeL il

-- add brackets and indent the lines of code
bracketBlock :: NonEmpty CodePP -> CodePP
bracketBlock = bracketCode . blockCode

-- surround with brackets and indent
bracketCode :: CodePP -> CodePP
bracketCode c = "{" <> PP.line <> PP.indent 2 c <> PP.line <> "} "

-- put each item of code on a separate line
blockCode :: NonEmpty CodePP -> CodePP
blockCode ne = PP.vsep $ NE.toList ne

-- put each line *after the first* on a new line
blockCode' :: NonEmpty CodePP -> CodePP
blockCode' (c :| cs) = c <> PP.vsep cs

appendList :: NonEmpty a -> [a] -> NonEmpty a
appendList (a :| as) as' = a :| (as ++ as')

functionArgs:: ArgTypeList args -> ArgList (FuncArg Text) args -> CodePP
functionArgs argTypes argNames = PP.parens $ mconcat $ PP.punctuate (PP.comma <> PP.space) argCodeList
  where
    handleFA :: CodePP -> FuncArg Text t -> CodePP
    handleFA c = \case
      Arg a -> c <+> PP.pretty a
      DataArg a -> "data" <+> c <+> PP.pretty a

    arrayIndices :: SNat n -> CodePP
    arrayIndices sn = if n == 0 then mempty else PP.brackets (mconcat $ List.replicate n PP.comma)
      where n = fromIntegral $ DT.snatToNatural sn

    handleType :: SType t -> CodePP
    handleType st = case st of
      SArray sn st -> "array" <> arrayIndices sn <+> handleType st
      _ -> PP.pretty $ sTypeName st

    f :: SType t -> FuncArg Text t -> K CodePP t
    f st fa = K $ handleFA (handleType st) fa

    argCodeList = argsKToList $ zipArgListsWith f (argTypesToSTypeList argTypes) argNames

ifElseCode :: NonEmpty (LExpr EBool, Either Text CodePP) -> Either Text CodePP -> Either Text CodePP
ifElseCode ecNE c = do
  let f = unK . exprToCode
      (conds, ifTrueCodeEs) = NE.unzip ecNE
      condCode (e :| es) = "if" <+> PP.parens (f e) <> PP.space :| fmap (\le -> "else if" <+> PP.parens (f le) <> PP.space) es
      condCodeNE = condCode conds `appendList` ["else"]
  ifTrueCodes <- sequenceA (ifTrueCodeEs `appendList` [c])
  let ecNE = NE.zipWith (\condCode ifTrueCode -> condCode <+> ifTrueCode)
             condCodeNE
             (fmap (bracketBlock . one) ifTrueCodes)
  return $ blockCode' ecNE

data  IExprCode :: Type where
  Unsliced :: CodePP -> IExprCode
  Oped :: SBinaryOp op -> CodePP -> IExprCode -- has an binary operator in it so might need parentheses
  Sliced :: CodePP -> IM.IntMap IExprCode -> IExprCode -- needs indexing

data IExprCodeF :: Type -> Type where
  UnslicedF :: CodePP -> IExprCodeF a
  OpedF :: SBinaryOp op -> CodePP -> IExprCodeF a
  SlicedF :: CodePP -> IM.IntMap a -> IExprCodeF a

instance Functor IExprCodeF where
  fmap f = \case
    UnslicedF doc -> UnslicedF doc
    OpedF b c -> OpedF b c
    SlicedF c im -> SlicedF c (f <$> im)

instance Foldable IExprCodeF where
  foldMap f = \case
    UnslicedF doc -> mempty
    OpedF _ _ -> mempty
    SlicedF c im -> foldMap f im

instance Traversable IExprCodeF where
  traverse g = \case
    UnslicedF doc -> pure $ UnslicedF doc
    OpedF b c -> pure $ OpedF b c
    SlicedF c im -> SlicedF c <$> traverse g im

type instance RS.Base IExprCode = IExprCodeF

instance RS.Recursive IExprCode where
  project = \case
    Unsliced doc -> UnslicedF doc
    Oped b c -> OpedF b c
    Sliced iec im -> SlicedF iec im

instance RS.Corecursive IExprCode where
  embed = \case
    UnslicedF doc -> Unsliced doc
    OpedF b c -> Oped b c
    SlicedF doc im -> Sliced doc im

iExprToDocAlg :: IExprCodeF CodePP -> CodePP
iExprToDocAlg = \case
  UnslicedF doc -> doc
  OpedF b c -> c
  SlicedF doc im -> doc <> PP.brackets (mconcat $ PP.punctuate ", " $ withLeadingEmpty im) -- or hsep?

withLeadingEmpty :: IntMap CodePP -> [CodePP]
withLeadingEmpty im = imWLE where
  minIndex = Foldable.minimum $ IM.keys im
  imWLE = List.replicate minIndex mempty ++ IM.elems im

iExprToCode :: IExprCode -> CodePP
iExprToCode = RS.cata iExprToDocAlg

csArgList :: ArgList (K CodePP) args -> CodePP
csArgList = mconcat . PP.punctuate (PP.comma <> PP.space) . argsKToList

-- I am not sure about/do not understand the quantified constraint here.
exprToDocAlg :: IAlg LExprF (K IExprCode) -- LExprF ~> K IExprCode
exprToDocAlg = K . \case
  LNamed txt st -> Unsliced $ PP.pretty txt
  LInt n -> Unsliced $ PP.pretty n
  LReal x -> Unsliced $ PP.pretty x
  LComplex x y -> Unsliced $ PP.parens $ PP.pretty x <+> "+" <+> "i" <> PP.pretty y -- (x + iy))
  LString t -> Unsliced $ PP.dquotes $ PP.pretty t
  LFunction (Function fn _ _) al -> Unsliced $ PP.pretty fn <> PP.parens (csArgList $ hfmap f al)
  LDistribution (Distribution dn _ _) k al -> Unsliced $ PP.pretty dn <> PP.parens (unK (f k) <> PP.pipe <+> csArgList (hfmap f al))
  LBinaryOp sbo le re -> Oped sbo $ unK (f $ parenthesizeOped le) <+> opDoc sbo <+> unK (f $ parenthesizeOped re)
  LUnaryOp op e -> Unsliced $ unaryOpDoc (unK (f $ parenthesizeOped e)) op
  LCond ce te fe -> Unsliced $ unK (f ce) <+> "?" <+> unK (f te) <+> PP.colon <+> unK (f fe)
  LSlice sn ie e -> sliced sn ie e
  where
    f :: K IExprCode ~> K CodePP
    f = K . iExprToCode . unK
    parenthesizeOped :: K IExprCode ~> K IExprCode
    parenthesizeOped x = case unK x of
       Oped bo doc -> K $ Oped bo $ PP.parens doc
       x -> K x
    slice :: SNat n -> K IExprCode EInt -> K IExprCode d -> IM.IntMap IExprCode -> IM.IntMap IExprCode
    slice sn kei ke im = im'
      where
        newIndex :: Int = fromIntegral $ DT.snatToNatural sn
        skip = IM.size $ IM.filterWithKey (\k _ -> k <= newIndex) im
        im' = IM.insert (newIndex + skip) (unK kei) im
    sliced :: SNat n -> K IExprCode EInt -> K IExprCode t -> IExprCode
    sliced sn kei ke = case unK ke of
      Unsliced c -> Sliced c $ slice sn kei ke $ IM.empty
      Oped _ c -> Sliced (PP.parens c) $ slice sn kei ke $ IM.empty
      Sliced c im -> Sliced c $ slice sn kei ke im

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
  SAndEqual sbo -> opDoc sbo <> PP.equals
  SBoolean bop -> boolOpDoc bop
