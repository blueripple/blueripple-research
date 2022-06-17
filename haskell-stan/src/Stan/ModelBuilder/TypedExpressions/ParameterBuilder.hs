{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE LambdaCase #-}

module Stan.ModelBuilder.TypedExpressions.ParameterBuilder
  (
    module Stan.ModelBuilder.TypedExpressions.ParameterBuilder
  )
  where

import Prelude hiding (All)
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Stan.ModelBuilder.BuilderTypes as SB
import qualified Stan.ModelBuilder as SB

import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.TypedList as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Functions as TE
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as TE
import qualified Stan.ModelBuilder.TypedExpressions.Expressions as TE
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TE
import Stan.ModelBuilder.TypedExpressions.Recursion (hfmap, htraverse, K(..))
import Data.Vec.Lazy (Vec(..))
import Stan.ModelBuilder.TypedExpressions.Types (TypeOneOf)
import Stan.ModelBuilder.TypedExpressions.Statements (DensityWithArgs)
import qualified Data.GADT.Compare as GC
import qualified Data.Type.Equality as GC
import qualified Data.Dependent.Map as DM
import qualified Data.Dependent.Sum as DM
import qualified Data.Graph as Gr
import qualified Data.Some as Some
import qualified Control.Foldl as FL

-- Transformed Data declarations can only depend on other transformed data, so we need
-- a wrapper type to enforce that.

--type Givens ts = TE.TypedList TE.UExpr ts

-- parameterized by the type of the parameter
-- Each can include statements to be added to
-- transformed data block
data Parameter :: TE.EType -> Type where
  GivenP :: TE.UExpr t -> Parameter t
  BuildP :: BuildParameter t -> Parameter t

type Parameters ts = TE.TypedList Parameter ts

data FunctionToDeclare = FunctionToDeclare Text TE.UStmt

data TData :: TE.EType -> Type where
  TData :: TE.NamedDeclSpec t -> [FunctionToDeclare] -> TE.TypedList TData ts -> (TE.ExprList ts -> TE.UExpr t) -> TData t

--withTData :: TData t -> (forall ts.TE.NamedDeclSpec t -> TE.TypedList TData ts -> (TE.ExprList ts -> TE.UExpr t) -> r) -> r
--withTData (TData nds tds eF) f = f nds tds eF

tDataNamedDecl :: TData t -> TE.NamedDeclSpec t
tDataNamedDecl (TData nds _ _ _) = nds


data BuildParameter :: TE.EType -> Type where
  TransformedDataP :: TData t -> BuildParameter t
  UntransformedP :: TE.NamedDeclSpec t
                 -> [FunctionToDeclare]
                 -> Parameters qs
                 -> TE.Density t qs
                 -> BuildParameter t
  TransformedSameTypeP :: TE.NamedDeclSpec t
                       -> [FunctionToDeclare]
                       -> Parameters qs
                       -> TE.Density t qs
                       -> Parameters ts
                       -> (TE.ExprList ts -> TE.UExpr t -> TE.UExpr t)
                       -> BuildParameter t
  TransformedDiffTypeP :: TE.NamedDeclSpec t
                       -> [FunctionToDeclare]
                       -> TE.DeclSpec q
                       -> Parameters qs
                       -> TE.Density q qs
                       -> Parameters ts
                       -> (TE.ExprList ts -> TE.UExpr q -> TE.UExpr t)
                       -> BuildParameter t

type BuildParameters ts = TE.TypedList BuildParameter ts


getNamedDecl :: BuildParameter t -> TE.NamedDeclSpec t --SB.StanBuilderM md gq (TE.NamedDeclSpec t)
getNamedDecl = \case
  TransformedDataP (TData nds _ _ _) -> nds
  UntransformedP x _ _ _ -> x
  TransformedSameTypeP x _ _ _ _ _ -> x
  TransformedDiffTypeP x _ _ _ _ _ _ -> x

setNamedDecl :: TE.NamedDeclSpec t -> BuildParameter t -> BuildParameter t --SB.StanBuilderM md gq (Parameter t)
setNamedDecl x = \case
  TransformedDataP (TData _ y z a) -> TransformedDataP (TData x y z a)
  UntransformedP _ y z a -> UntransformedP x y z a
  TransformedSameTypeP _ y z a b c -> TransformedSameTypeP x y z a b c
  TransformedDiffTypeP _ y z a b c d -> TransformedDiffTypeP x y z a b c d

bParameterName :: BuildParameter t -> TE.StanName
bParameterName = TE.declName . getNamedDecl

bParameterSType :: BuildParameter t -> TE.SType t
bParameterSType = TE.sTypeFromStanType . TE.declType . TE.decl . getNamedDecl

data TypeTaggedName :: TE.EType -> Type where
  TypeTaggedName :: TE.SType t -> TE.StanName -> TypeTaggedName t

taggedType :: TypeTaggedName t -> TE.SType t
taggedType (TypeTaggedName st _) = st

taggedName :: TypeTaggedName t -> TE.StanName
taggedName (TypeTaggedName _ n ) = n

instance GC.GEq TypeTaggedName where
  geq a b  = GC.testEquality (taggedType a) (taggedType b)

instance GC.GCompare TypeTaggedName where
  gcompare a b = case GC.geq a b of
    Just GC.Refl -> case compare (taggedName a) (taggedName b) of
      EQ -> GC.GEQ
      LT -> GC.GLT
      GT -> GC.GGT

pTypeTaggedName :: BuildParameter t -> TypeTaggedName t
pTypeTaggedName p = TypeTaggedName (bParameterSType p) (bParameterName p)

data ParameterCollection = ParameterCollection { pdm :: DM.DMap TypeTaggedName BuildParameter, usedNames :: Set TE.StanName }

-- we don't add givens since they are already...given
addParameterToCollection' :: Parameter t -> ParameterCollection -> Either Text ParameterCollection
addParameterToCollection' p pc =
  case p of
    GivenP {} -> Right pc
    BuildP bp -> do
      let pName = bParameterName bp
          pSType = bParameterSType bp

      if Set.member pName (usedNames pc)
        then Left $ "Attempt to add " <> pName <> "to parameter collection but a parameter of that name is already present."
        else Right $ ParameterCollection (DM.insert (pTypeTaggedName bp) bp $ pdm pc) (Set.insert pName $ usedNames pc)

-- Parameter Dependencies types are scoped to stay within a `Parameter t`
-- so to do anything which uses them, we need to use CPS
withBPDeps :: BuildParameter t -> (forall ts.Parameters ts -> r) -> r
withBPDeps (TransformedDataP (TData _ _ tds _)) f = f $ hfmap (BuildP . TransformedDataP) tds
withBPDeps (UntransformedP _ _ ps _) f = f ps
withBPDeps (TransformedSameTypeP _ _ pq _ pt _) f = f (pq `TE.appendTypedLists` pt)
withBPDeps (TransformedDiffTypeP _ _ _ pq _ pt _) f = f (pq `TE.appendTypedLists` pt)

data PhantomP where
  PhantomP :: forall t. BuildParameter t -> PhantomP

withPhantomP :: PhantomP -> (forall t. BuildParameter t -> r) -> r
withPhantomP (PhantomP p) f = f p

-- we build a graph, using wrapped parameters as nodes and names as keys
-- topologically sort it
-- return the list of parameters in an order we can build them.
depOrderedPParameters :: ParameterCollection -> [PhantomP]
depOrderedPParameters pc =  (\(pp, _, _) -> pp) . vToBuildInfo <$> Gr.topSort pGraph where
  parameterNameM :: Parameter t -> Maybe TE.StanName
  parameterNameM = \case
    GivenP _ -> Nothing
    BuildP bp -> Just $ bParameterName bp
  bParameterNames :: Parameters ts -> [TE.StanName]
  bParameterNames = catMaybes . TE.typedKToList . hfmap (K . parameterNameM)
  dSumToGBuildInfo :: DM.DSum TypeTaggedName BuildParameter -> (PhantomP, TE.StanName, [TE.StanName])
  dSumToGBuildInfo (_ DM.:=> bp) = (PhantomP bp, bParameterName bp, withBPDeps bp bParameterNames)
  (pGraph, vToBuildInfo, nameToVertex) = Gr.graphFromEdges . fmap dSumToGBuildInfo . DM.toList $ pdm pc
  orderedVList = Gr.topSort pGraph

lookupParameterExpressions :: Parameters ts -> DM.DMap TypeTaggedName TE.UExpr -> Either Text (TE.TypedList TE.UExpr ts)
lookupParameterExpressions ps eMap = htraverse f ps where
  f :: Parameter t -> Either Text (TE.UExpr t)
  f p = case p of
    GivenP e -> return e
    BuildP bp -> do
      case DM.lookup (pTypeTaggedName bp) eMap of
        Just e -> Right e
        Nothing -> Left $ bParameterName bp <> " not found in expression map.  Dependency ordering issue??"

-- This bit needs to move to someplace else so we can not import ModelBuilder
addParameterToCodeAndMap :: DM.DMap TypeTaggedName TE.UExpr
                         -> PhantomP
                         -> SB.StanBuilderM md gq (DM.DMap TypeTaggedName TE.UExpr)
addParameterToCodeAndMap eMap (PhantomP bp) = do
  v <- case bp of
    TransformedDataP (TData nds ftds tds eF) -> do
      traverse_ (\(FunctionToDeclare n fs) -> SB.addFunctionsOnce n $ SB.addStmtToCode fs) ftds
      tdEs <- SB.stanBuildEither $ lookupParameterExpressions (hfmap (BuildP . TransformedDataP) tds) eMap
      SB.inBlock SB.SBTransformedData $ SB.stanDeclareRHSN nds $ eF tdEs
    UntransformedP nds ftds ps d -> do
      traverse_ (\(FunctionToDeclare n fs) -> SB.addFunctionsOnce n $ SB.addStmtToCode fs) ftds
      psE <- SB.stanBuildEither $ lookupParameterExpressions ps eMap
      v <-  SB.inBlock SB.SBParameters $ SB.stanDeclareN nds
      SB.inBlock SB.SBModel $ SB.addStmtToCode $ TE.sample v d psE
      return v
    TransformedSameTypeP nds ftds pd d pt tF -> do
      traverse_ (\(FunctionToDeclare n fs) -> SB.addFunctionsOnce n $ SB.addStmtToCode fs) ftds
      vRaw <- SB.inBlock SB.SBParameters $ SB.stanDeclare (rawName $ TE.declName nds) $ TE.decl nds
      pdEs <- SB.stanBuildEither $ lookupParameterExpressions pd eMap
      SB.inBlock SB.SBModel $ SB.addStmtToCode $ TE.sample vRaw d pdEs
      ptEs <- SB.stanBuildEither $ lookupParameterExpressions pt eMap
      SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHSN nds $ tF ptEs vRaw
    TransformedDiffTypeP nds ftds dq pd d pt tF -> do
      traverse_ (\(FunctionToDeclare n fs) -> SB.addFunctionsOnce n $ SB.addStmtToCode fs) ftds
      pdEs <- SB.stanBuildEither $ lookupParameterExpressions pd eMap
      vRaw <- SB.inBlock SB.SBParameters $ SB.stanDeclare (rawName $ TE.declName nds) dq
      SB.inBlock SB.SBModel $ SB.addStmtToCode $ TE.sample vRaw d pdEs
      tEs <- SB.stanBuildEither $ lookupParameterExpressions pt eMap
      SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHSN nds $ tF tEs vRaw
  return $ DM.insert (pTypeTaggedName bp) v eMap


addAllParametersInCollection :: forall md gq. ParameterCollection -> SB.StanBuilderM md gq ()
addAllParametersInCollection = FL.foldM makeFold . depOrderedPParameters
  where makeFold :: FL.FoldM (SB.StanBuilderM x y) PhantomP ()
        makeFold = FL.FoldM addParameterToCodeAndMap (pure DM.empty) (const $ pure ())

rawName :: Text -> Text
rawName t = t <> "_raw"
--

exprListToParameters :: TE.ExprList ts  -> Parameters ts
exprListToParameters = hfmap GivenP
