package org.jetbrains.service

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import feign.FeignException
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.jetbrains.recommender.RandomCoffeeApiClient
import org.jetbrains.recommender.model.SuggestCatForRandomCoffeeRequest
import org.jetbrains.repository.Cat
import org.jetbrains.repository.CatBreedRepository
import org.jetbrains.repository.CatRepository
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

data class CatWithBreed(val id: Long, val name: String, val breed: String)
data class NewCat(val name: String, val breed: String)

@Serviceclass CatService(
    private val catRepository: CatRepository,
    private val catBreedRepository: CatBreedRepository,
    private val catRecommenderClient: RandomCoffeeApiClient
) {

    private val log: Logger = LoggerFactory.getLogger(CatService::class.java)

    fun generatePairs(limit: Int): List<Pair<CatWithBreed, CatWithBreed>> {
        return try {
            val cats = catRepository.findAllWithLimit(limit)
            val breedIds = cats.mapNotNull { it.breedId }.distinct()
            
            val breedsMap = catBreedRepository.findAllById(breedIds).associateBy { it.id }
            
            val catList = cats.mapNotNull { cat ->
                val breed = breedsMap[cat.breedId]
                if (breed == null) {
                    log.warn("Breed not found for cat with id: ${cat.id}, breedId: ${cat.breedId}. Skipping this cat.")
                    null
                } else {
                    CatWithBreed(
                        cat.id,
                        cat.name,
                        breed.name
                    )
                }
            }
            
            if (catList.isEmpty()) {
                log.warn("No valid cats found after breed lookup. Returning empty list.")
                emptyList()
            } else {
                catList
            }
        } catch (e: Exception) {
            log.error("Error occurred while generating pairs", e)
            emptyList()
        }
    }
}return catList.mapNotNull { cat ->
            try {
                val friendId = suggestCat(cat)
                cat to friendId
            } catch (e: FeignException) {
                logger.warn("Failed to get cat recommendation for cat: ${cat.id}", e)
                null
            }
        }
            .mapNotNull { (cat, friendId) ->
                val friend = catRepository.findById(friendId).orElse(null)
                if (friend == null) {
                    logger.warn("Cat not found with id: $friendId")
                    null
                } else {
                    cat to friend
                }
            }
            .let { catFriendPairs ->
                val breedIds = catFriendPairs.map { it.second.breedId }.distinct()
                val breedsMap = catBreedRepository.findAllById(breedIds).associateBy { it.id }
                catFriendPairs.mapNotNull { (cat, friend) ->
                    val breed = breedsMap[friend.breedId]
                    if (breed == null) {
                        logger.warn("Breed not found for breed id: ${friend.breedId}, skipping cat: ${friend.id}")
                        null
                    } else {
                        cat to CatWithBreed(
                            friend.id,
                            friend.name,
                            breed.name
                        )
                    }
                }
            }
    }

    @Transactional
    fun addCat(cat: NewCat): CatWithBreed {
        val breed = catBreedRepository.findByName(cat.breed).orElseThrow { RuntimeException("Breed not found") }
        val createdCat = catRepository.save(Cat(0L, breed.id, cat.name, ""))
        return CatWithBreed(createdCat.id, createdCat.name, breed.name)
    }@WithSpan
    private fun suggestCat(cat: CatWithBreed): Long? =
        try {
            catRecommenderClient.suggestCat(SuggestCatForRandomCoffeeRequest(cat.id, cat.name, cat.breed)).id
        } catch (e: FeignException) {
            log.error("Failed to suggest cat with id: ${cat.id}, name: ${cat.name}", e)
            null
        }

    @WithSpan
    @Transactional
    fun findCatsByName(name: String): List<CatWithBreed> {
        val cats = catRepository.findAllByName(name)
        val breedIds = cats.map { it.breedId }.distinct()
        val breedsMap = catBreedRepository.findAllById(breedIds).associateBy { it.id }
        
        val result = cats.mapNotNull { cat ->
            val breed = breedsMap[cat.breedId]
            if (breed == null) {
                log.warn("Breed not found for cat id: ${cat.id}, breedId: ${cat.breedId}. Skipping this cat.")
                null
            } else {
                CatWithBreed(
                    cat.id,
                    cat.name,
                    breed.name
                )
            }
        }
        return result
    }@WithSpan
    @Transactional
    fun getAllCats(): List<CatWithBreed> {
        val allCats = catRepository.findAll()
        val breedIds = allCats.map { it.breedId }.distinct()
        val breedsMap = catBreedRepository.findAllById(breedIds).associateBy { it.id }
        
        return allCats.mapNotNull { cat ->
            val breed = breedsMap[cat.breedId]
            if (breed == null) {
                logger.warn("Breed not found for cat with id: ${cat.id}, breedId: ${cat.breedId}. Skipping this cat.")
                null
            } else {
                CatWithBreed(
                    cat.id,
                    cat.name,
                    breed.name
                )
            }
        }
    }

    @WithSpan
    @Transactional
    fun countCats(): Long = catRepository.countAll()

    companion object {
        private val JACKSON_MAPPER = ObjectMapper()
    }
}

data class FastAPIExceptionResponse(@JsonProperty("detail") val detail: String)class CatRecommenderIntegrationException(message: String, exception: Exception) : RuntimeException(message, exception)