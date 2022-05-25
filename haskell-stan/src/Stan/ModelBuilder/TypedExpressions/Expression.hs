{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# LANGUAGE TypeSynonymInstances #-}

module Stan.ModelBuilder.TypedExpressions.Expression
  (
    module Stan.ModelBuilder.TypedExpressions.Expression
  , Nat(..)
  , Fin(..)
  , Vec(..)
  )
  where

import qualified Stan.ModelBuilder.TypedExpressions.Recursion as TR
import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.Indexing
import Stan.ModelBuilder.TypedExpressions.Arithmetic

import Prelude hiding (Nat)
import qualified Data.Functor.Classes as FC
import qualified Data.Functor.Classes.Generic as FC
import qualified Data.Functor.Foldable as Rec
import qualified Data.Functor.Foldable.Monadic as Rec
import qualified Data.Functor.Foldable.TH as Rec
import           Data.Kind (Type)
import qualified Data.Fix as Fix
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import qualified Data.Text as T

import Data.Type.Nat (Nat(..), SNat(..))
import Data.Fin (Fin(..))
import Data.Vec.Lazy (Vec(..))

import qualified Data.Nat as DT
import qualified Data.Type.Nat as DT
import qualified Data.Type.Nat.LE as DT
import qualified Data.Fin as DT hiding (split)
import qualified Data.Vec.Lazy as DT

import qualified Text.PrettyPrint as Pretty

import GHC.Generics (Generic1)
import qualified GHC.TypeLits as TE
import GHC.TypeLits (ErrorMessage((:<>:)))
import qualified Stan.ModelBuilder.Expressions as SME
import Stan.ModelBuilder.Expressions (StanVar(..))
import Stan.ModelBuilder (DataSetGroupIntMaps)
import Frames.Streamly.CSV (accToMaybe)
import Knit.Report (moveOriginBy)
import qualified Data.Constraint.Deferrable as DT
import qualified Data.Type.Nat.LE as ST
import Stan.ModelConfig (DataIndexerType(NoIndex))
import Data.Vinyl.Functor (Lift)

type DeclIndexVec s et = DeclIndexVecF s UExpr et--Vec (UExpr EInt) (DeclDimension et)

type IndexVec s et = IndexVecF s UExpr et -- (UExpr EInt) (Dimension et)

data UExprF :: (Ty -> Type) -> Ty -> Type where
  DeclareEF :: StanType t -> DeclIndexVecF s r et -> UExprF r ('Ty s et)
  NamedEF :: Text -> SType t -> UExprF r ('Ty EVar t)
  IntEF :: Int -> UExprF r ('Ty ELit EInt)
  RealEF :: Double -> UExprF r ('Ty ELit EReal)
  ComplexEF :: Double -> Double -> UExprF r ('Ty ELit EComplex)
  BinaryOpEF :: SBinaryOp op -> r ta -> r tb -> UExprF r ('Ty ECompound (BinaryResultT op (TyType ta) (TyType tb)))
  SliceEF :: SNat n -> r ('Ty s EInt) -> r t -> UExprF r ('Ty (TyStructure t) (Sliced n (TyType t)))
  NamedIndexEF :: SME.IndexKey -> UExprF r ('Ty ELookup EInt)

instance TR.NFunctor UExprF where
  nmap nat = \case
    DeclareEF st vec -> DeclareEF st (TR.nmap nat vec)
    NamedEF txt st -> NamedEF txt st
    IntEF n -> IntEF n
    RealEF x -> RealEF x
    ComplexEF rp ip -> ComplexEF rp ip
    BinaryOpEF sbo gta gtb -> BinaryOpEF sbo (nat gta) (nat gtb)
    SliceEF sn g gt -> SliceEF sn (nat g) (nat gt)
    NamedIndexEF txt -> NamedIndexEF txt

type UExpr = TR.IFix UExprF

declareE :: StanType t -> DeclIndexVec s t -> UExpr ('Ty s t)
declareE st = TR.IFix . DeclareEF st

namedE :: Text -> SType t -> UExpr ('Ty EVar t)
namedE name  = TR.IFix . NamedEF name

intE :: Int -> UExpr ('Ty ELit EInt)
intE = TR.IFix . IntEF

realE :: Double -> UExpr ('Ty ELit EReal)
realE = TR.IFix . RealEF

complexE :: Double -> Double -> UExpr ('Ty ELit EComplex)
complexE rp ip = TR.IFix $ ComplexEF rp ip

binaryOpE :: SBinaryOp op -> UExpr ta -> UExpr tb -> UExpr ('Ty ECompound (BinaryResultT op (TyType ta) (TyType tb)))
binaryOpE op ea eb = TR.IFix $ BinaryOpEF op ea eb

sliceE :: SNat n -> UExpr ('Ty s EInt) -> UExpr t -> UExpr ('Ty (TyStructure t) (Sliced n (TyType t))) --(Sliced n t)
sliceE sn ie e = TR.IFix $ SliceEF sn ie e

namedIndexE :: Text -> UExpr ('Ty ELookup EInt)
namedIndexE = TR.IFix . NamedIndexEF


data LExprF :: (Ty -> Type) -> Ty -> Type where
  LDeclareF :: StanType t -> DeclIndexVecF s r et -> LExprF r ('Ty s et)
  LNamedF :: Text -> SType t -> LExprF r ('Ty EVar t)
  LIntF :: Int -> LExprF r ('Ty ELit EInt)
  LRealF :: Double -> LExprF r ('Ty ELit EReal)
  LComplexF :: Double -> Double -> LExprF r ('Ty ELit EComplex)
  LBinaryOpF :: SBinaryOp op -> r ta -> r tb -> LExprF r ('Ty ECompound (BinaryResultT op (TyType ta) (TyType tb)))
  LSliceF :: SNat n -> r ('Ty s EInt) -> r t -> LExprF r ('Ty (TyStructure t) (Sliced n (TyType t)))

instance TR.NFunctor LExprF where
  nmap nat = \case
    LDeclareF st vec -> LDeclareF st (TR.nmap nat vec)
    LNamedF txt st -> LNamedF txt st
    LIntF n -> LIntF n
    LRealF x -> LRealF x
    LComplexF rp ip -> LComplexF rp ip
    LBinaryOpF sbo gta gtb -> LBinaryOpF sbo (nat gta) (nat gtb)
    LSliceF sn g gt -> LSliceF sn (nat g) (nat gt)

type LExpr = TR.IFix LExprF


{-

indexInner :: UExprF t a -> UExprF EInt a -> UExprF (SliceInnerN (S Z) t) a
indexInner e i = SliceE SZ i e

-- NB: We need the "go" here to add the SNat to the steps so GHC can convince itself that the lengths match up
-- This will yield a compile-time error if we try to index past the end or, same same, index something scalar.
-- That is, if n > Dimension a, this cannot be compiled.
indexInnerN :: UExprF t a -> Vec (UExprF EInt a) n -> UExprF (SliceInnerN n t) a
indexInnerN e v = go (sLength v) e v where
  go :: SNat n -> UExprF tb b -> Vec (UExprF EInt b) n -> UExprF (SliceInnerN n tb) b
  go SZ e _ = e
  go (SS m) e (i :> v') = go (sLength v') (indexInner e i) v'

indexAll :: UExprF t a -> IndexVec t a -> UExprF (SliceInnerN (Dimension t) t) a
indexAll = indexInnerN

plusE :: UExprF ta a -> UExprF tb a -> UExprF (BinaryResultT BAdd ta tb) a
plusE = BinaryOpE SAdd
-}

{-
useVar :: forall r.SME.StanVar -> (forall t.UExpr t -> r) -> r
useVar (SME.StanVar n x) k = case x of
  StanInt -> k $ NamedE n x SInt
  StanReal -> k $ NamedE n x SReal
  StanArray _ st -> toSType (fromStanType st) (k . NamedE n x . SArray)
  StanVector _ -> k $ NamedE n x SCVec
  StanMatrix _ -> k $ NamedE n x SMat
  StanCorrMatrix sd -> k $ NamedE n x SMat
  StanCholeskyFactorCorr sd -> k $ NamedE n x SMat
  StanCovMatrix sd -> k $ NamedE n x SMat
-}
{-
data StanType = StanInt
              | StanReal
              | StanArray [SME.StanDim] StanType
              | StanVector SME.StanDim
              | StanMatrix (SME.StanDim, SME.StanDim)
              | StanCorrMatrix SME.StanDim
              | StanCholeskyFactorCorr SME.StanDim
              | StanCovMatrix SME.StanDim
              deriving (Show, Eq, Ord, Generic)

fromStanType :: StanType -> EType
fromStanType = \case
  StanInt -> EInt
  StanReal -> EReal
  StanArray _ st -> EArray (fromStanType st)
  StanVector _ -> ECVec
  StanMatrix _ -> EMat
  StanCorrMatrix _ -> EMat
  StanCholeskyFactorCorr _ -> EMat
  StanCovMatrix _ -> EMat
-}
{-
class ToEType a where
  toEType :: EType

instance ToEType SInt where
  toEType = EInt
instance ToEType SInt where

  toEType SReal = EReal
  toEType SCVec = ECVec
  toEType SRVec = ERVec
  toEType SMat = EMat
  toEType (SArray st) = EArray (toEType st)
  toEType (sa ::-> sb) = toEType sa :-> toEType sb
-}
{-
  toSType (fromStanType x) f where
  f :: forall u.SType u -> r
  f = case x of
    StanInt -> k $ UNamedE n x SInt
    StanReal -> k $ UNamedE n x SReal
    StanArray _ st -> k $ toSType (fromStanType st) $ UNamedE n x . SArray
    StanVector sd -> _
    StanMatrix x1 -> _
    StanCorrMatrix sd -> _
    StanCholeskyFactorCorr sd -> _
    StanCovMatrix sd -> _
-}



{-
intE :: Int -> UExpr EInt
intE = IntE

realE :: Double -> UExpr EReal
realE = RealE

varE :: SType t -> Text -> UExpr t
varE _ = VarE

plusOpE :: UExpr t -> UExpr (t :-> t)
plusOpE a = FunE ()
-}
