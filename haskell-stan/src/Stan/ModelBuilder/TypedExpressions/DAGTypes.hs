{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE LambdaCase #-}

module Stan.ModelBuilder.TypedExpressions.DAGTypes
  (
    ParameterTag
  , taggedParameterName
  , taggedParameterType
  , parameterExpr
  , parameterTagExpr
  , FunctionToDeclare(..)
  , TData(..)
  , Parameter(..)
  , given
  , build
  , mapped
  , Parameters
  , tagsAsExprs
  , parametersAsExprs
  , DeclCode(..)
  , BuildParameter(..)
  , BParameterCollection(..)
  , bParameterName
  , bParameterSType
  , bParameterStanType
  , addBuildParameterE
  , withBPDeps
  , lookupParameterExpressions
  , lookupTDataExpressions
  , addBuiltExpressionToMap
  )
  where

import Prelude hiding (All)

import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.TypedList as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import Stan.ModelBuilder.TypedExpressions.Recursion (hfmap, htraverse)
import qualified Data.GADT.Compare as GC
import qualified Data.GADT.Show as GC
import qualified Data.Type.Equality as GC
import qualified Data.Dependent.Map as DM

import Data.Type.Equality ((:~:)(Refl),TestEquality(testEquality))
import qualified Text.Show
import qualified Data.Set as Set
import Stan.ModelBuilder.TypedExpressions.Expressions (UExpr)
import Stan.ModelBuilder.TypedExpressions.Types (sTypeToEType)

-- ultimately, we should not expose this constructor.  So to get one of these you have to add a Builder to the DMap.
data ParameterTag :: TE.EType -> Type where
  ParameterTag :: TE.SType t -> TE.StanName -> ParameterTag t

taggedParameterType :: ParameterTag t -> TE.SType t
taggedParameterType (ParameterTag st _) = st

taggedParameterName :: ParameterTag t -> TE.StanName
taggedParameterName (ParameterTag _ n ) = n

instance GC.GEq ParameterTag where
  geq a b  = GC.testEquality (taggedParameterType a) (taggedParameterType b)

instance GC.GCompare ParameterTag where
  gcompare a b = case GC.geq a b of
    Just GC.Refl -> case compare (taggedParameterName a) (taggedParameterName b) of
      EQ -> GC.GEQ
      LT -> GC.GLT
      GT -> GC.GGT
    Nothing -> case compare (sTypeToEType $ taggedParameterType a) (sTypeToEType $ taggedParameterType b) of
      LT -> GC.GLT
      GT -> GC.GGT

instance Show (ParameterTag t) where
  show (ParameterTag st n) = "<" <> show st <> ": " <> show n <> ">"

instance GC.GShow ParameterTag where gshowsPrec = Text.Show.showsPrec

parameterTagFromBP :: BuildParameter t -> ParameterTag t
parameterTagFromBP p = ParameterTag (bParameterSType p) (bParameterName p)

parameterTagExpr :: ParameterTag t -> TE.UExpr t
parameterTagExpr (ParameterTag st n) = TE.namedE n st

--data UseParameter :: TE.EType -> Type where
--  AsIs :: ParameterTag t -> UseParameter t
--  Mapped :: (TE.UExpr t -> TE.UExpr t') -> UseParameter t -> UseParameter t'



--useParameterExpr :: UseParameter t -> UExpr t
--useParameterExpr (AsIs pt) = parameterTagExpr pt
--useParameterExpr (Mapped g pt) = g $ useParameterExpr pt

--mapParameter :: (TE.UExpr t -> TE.UExpr t') -> UseParameter t -> UseParameter t'
--mapParameter = Mapped

-- Transformed Data declarations can only depend on other transformed data, so we need
-- a wrapper type to enforce that.

--type Givens ts = TE.TypedList TE.UExpr ts

-- parameterized by the type of the parameter
-- Each can include statements to be added to
-- transformed data block
data Parameter :: TE.EType -> Type where
  GivenP :: TE.UExpr t -> Parameter t
  BuildP :: ParameterTag t -> Parameter t
  MappedP :: (TE.UExpr t -> TE.UExpr t') -> Parameter t -> Parameter t'

parameterExpr :: Parameter t -> TE.UExpr t
parameterExpr (GivenP e) = e
parameterExpr (BuildP p) = parameterTagExpr p
parameterExpr (MappedP g p) = g $ parameterExpr p

given :: TE.UExpr t -> Parameter t
given = GivenP

build :: ParameterTag t -> Parameter t
build = BuildP

mapped :: (UExpr t -> UExpr t') -> Parameter t -> Parameter t'
mapped = MappedP

type Parameters ts = TE.TypedList Parameter ts

tagsAsExprs :: TE.TypedList ParameterTag ts -> TE.ExprList ts
tagsAsExprs = hfmap parameterTagExpr
{-# INLINEABLE tagsAsExprs #-}

parametersAsExprs :: Parameters ts -> TE.ExprList ts
parametersAsExprs = hfmap parameterExpr
{-# INLINEABLE parametersAsExprs #-}

data FunctionToDeclare = FunctionToDeclare Text TE.UStmt

data DeclCode t where
  DeclRHS :: TE.UExpr t -> DeclCode t
  DeclCodeF :: (TE.UExpr t -> [TE.UStmt]) -> DeclCode t


data TData :: TE.EType -> Type where
  TData :: TE.NamedDeclSpec t
        -> [FunctionToDeclare]
        -> TE.TypedList TData ts
        -> (TE.ExprList ts -> DeclCode t) -- code for the transformed data block
        -> TData t

parameterTagFromTData :: TData t -> ParameterTag t
parameterTagFromTData (TData (TE.NamedDeclSpec n (TE.DeclSpec st _ _)) _ _ _) = ParameterTag (TE.sTypeFromStanType st) n

instance TestEquality TData where
  testEquality tda tdb = testEquality (f tda) (f tdb) where
    f (TData (TE.NamedDeclSpec _ (TE.DeclSpec st _ _)) _ _ _) = TE.sTypeFromStanType st


--withTData :: TData t -> (forall ts.TE.NamedDeclSpec t -> TE.TypedList TData ts -> (TE.ExprList ts -> TE.UExpr t) -> r) -> r
--withTData (TData nds tds eF) f = f nds tds eF

tDataNamedDecl :: TData t -> TE.NamedDeclSpec t
tDataNamedDecl (TData nds _ _ _) = nds

data BuildParameter :: TE.EType -> Type where
  TransformedDataP :: TData t -> BuildParameter t
  UntransformedP :: TE.NamedDeclSpec t
                 -> [FunctionToDeclare]
                 -> Parameters qs
                 -> (TE.ExprList qs -> TE.UExpr t -> [TE.UStmt]) -- prior in model block
                 -> BuildParameter t
  TransformedP :: TE.NamedDeclSpec t
               -> [FunctionToDeclare]
               -> Parameters qs
               -> (TE.ExprList qs -> DeclCode t) -- code for transformed parameters block
               -> BuildParameter t
  ModelP :: TE.NamedDeclSpec t
         -> [FunctionToDeclare]
         -> Parameters qs
         -> (TE.ExprList qs -> DeclCode t)
         -> BuildParameter t

instance TestEquality BuildParameter where
  testEquality bpa bpb = testEquality (f bpa) (f bpb) where
    f = TE.sTypeFromStanType . TE.declType . TE.decl . getNamedDecl

-- Parameter Dependencies types are scoped to stay within a `Parameter t`
-- so to do anything which uses them, we need to use CPS
withBPDeps :: BuildParameter t -> (forall ts. Parameters ts -> r) -> r
withBPDeps (TransformedDataP (TData _ _ tds _)) f = f $ hfmap (BuildP . parameterTagFromTData) tds
withBPDeps (UntransformedP _ _ ps _) f = f ps
withBPDeps (TransformedP _ _ pq _ ) f = f pq
withBPDeps (ModelP _ _ pq _ ) f = f pq

data BParameterCollection = BParameterCollection { pdm :: DM.DMap ParameterTag BuildParameter, usedNames :: Set TE.StanName }

--type BuildParameters ts = TE.TypedList BuildParameter ts

getNamedDecl :: BuildParameter t -> TE.NamedDeclSpec t --SB.StanBuilderM md gq (TE.NamedDeclSpec t)
getNamedDecl = \case
  TransformedDataP (TData nds _ _ _) -> nds
  UntransformedP x _ _ _ -> x
  TransformedP x _ _ _ -> x
  ModelP x _ _ _ -> x
--  TransformedDiffTypeP x _ _ _ _ _ _ -> x

setNamedDecl :: TE.NamedDeclSpec t -> BuildParameter t -> BuildParameter t --SB.StanBuilderM md gq (Parameter t)
setNamedDecl x = \case
  TransformedDataP (TData _ y z a) -> TransformedDataP (TData x y z a)
  UntransformedP _ y z a -> UntransformedP x y z a
  TransformedP _ y z a  -> TransformedP x y z a
  ModelP _ y z a  -> ModelP x y z a

--  TransformedDiffTypeP _ y z a b c d -> TransformedDiffTypeP x y z a b c d

bParameterName :: BuildParameter t -> TE.StanName
bParameterName = TE.declName . getNamedDecl

bParameterStanType :: BuildParameter t -> TE.StanType t
bParameterStanType = TE.declType . TE.decl . getNamedDecl

bParameterSType :: BuildParameter t -> TE.SType t
bParameterSType = TE.sTypeFromStanType . bParameterStanType

addBuildParameterE :: BuildParameter t -> BParameterCollection -> Either Text (BParameterCollection, ParameterTag t)
addBuildParameterE bp bpc = do
  let pName =  bParameterName bp
      pSType = bParameterSType bp
  if Set.member pName (usedNames bpc)
    then Left $ "Attempt to add " <> pName <> "to parameter collection but a parameter of that name is already present."
    else Right $ let ttn = parameterTagFromBP bp in (BParameterCollection (DM.insert ttn bp $ pdm bpc) (Set.insert pName $ usedNames bpc), ttn)

lookupParameterExpressions :: Parameters ts -> DM.DMap ParameterTag TE.UExpr -> Either Text (TE.TypedList TE.UExpr ts)
lookupParameterExpressions ps eMap = htraverse f ps where
  f :: Parameter t -> Either Text (TE.UExpr t)
  f p = case p of
      GivenP e -> return e
      BuildP ttn -> do
        case DM.lookup ttn eMap of
          Just e -> Right e
          Nothing -> Left $ taggedParameterName ttn <> " not found in expression map.  Dependency ordering issue??"
      MappedP g p -> g <$> f p
--    MappedP g p -> g <$> f p

lookupTDataExpressions :: TE.TypedList TData ts -> DM.DMap ParameterTag TE.UExpr -> Either Text (TE.TypedList TE.UExpr ts)
lookupTDataExpressions tds = lookupParameterExpressions (hfmap (BuildP . parameterTagFromTData) tds)

addBuiltExpressionToMap :: BuildParameter t -> TE.UExpr t -> DM.DMap ParameterTag UExpr -> DM.DMap ParameterTag UExpr
addBuiltExpressionToMap bp  =  DM.insert (parameterTagFromBP bp)
